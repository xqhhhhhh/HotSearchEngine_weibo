from __future__ import annotations

import base64
import json
import math
import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote

import scrapy
from scrapy.exceptions import CloseSpider
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from twisted.internet.error import TimeoutError

from weibo_hot.items import WeiboHotItem

try:
    import orjson

    _json_loads = orjson.loads
except Exception:
    _json_loads = json.loads


class WeiboTotalSpider(scrapy.Spider):
    name = "weibo_total"
    allowed_domains = ["hotengineapi.zhaoyizhe.com", "weibo.zhaoyizhe.com"]

    base_url = "https://hotengineapi.zhaoyizhe.com/hotEngineApi"
    aes_key = b"cce1d5a8d58249048623eb26b8b0ea53"

    def __init__(
        self,
        start_date: str = None,
        end_date: str = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        if start_date is None:
            start_date = os.getenv("START_DATE")
        if end_date is None:
            end_date = os.getenv("END_DATE")
        start_date = start_date or "2019-10-25"
        end_date = end_date or "2025-12-31"
        self.start_date = self._parse_date(start_date)
        self.end_date = self._parse_date(end_date)
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider._init_from_settings(crawler.settings)
        return spider

    def _init_from_settings(self, settings):
        self.date_step_days = int(settings.get("DATE_STEP_DAYS", 1))
        self.page_size = int(settings.get("PAGE_SIZE", 100))
        self.fetch_trend = bool(settings.get("FETCH_TREND", True))
        self.trend_source = str(settings.get("TREND_SOURCE", "superInfo")).strip()
        self.trend_timeout = int(settings.get("TREND_TIMEOUT", 60))
        self.failed_urls_path = str(settings.get("FAILED_URLS_PATH", "output/failed_urls.txt"))
        self._aes_cipher = AES.new(self.aes_key, AES.MODE_ECB)

        self.cookie = settings.get("WEIBO_COOKIE", "")
        if not self.cookie:
            self.logger.warning("WEIBO_COOKIE is empty; requests may fail.")

        self._init_trend_cache()
        self.pending = {}

    def _init_trend_cache(self) -> None:
        self.trend_cache_path = self.settings.get("TREND_CACHE_PATH", "trend_cache.sqlite")
        self.conn = sqlite3.connect(self.trend_cache_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trend_cache_minute (
                topic TEXT PRIMARY KEY,
                first_date TEXT,
                last_date TEXT,
                duration_minutes INTEGER,
                points INTEGER,
                updated_at TEXT
            )
            """
        )
        self.conn.commit()

    @staticmethod
    def _parse_date(s: str) -> date:
        return datetime.strptime(s, "%Y-%m-%d").date()

    def _build_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Origin": "https://weibo.zhaoyizhe.com",
            "Referer": "https://weibo.zhaoyizhe.com/",
            "User-Agent": self.settings.get("USER_AGENT"),
        }
        if self.cookie:
            headers["Cookie"] = self.cookie
            m = re.search(r"(?:^|;\s*)wbrsnew=([^;]+)", self.cookie)
            if m:
                headers["wbrsnew"] = m.group(1)
        return headers

    def _decrypt(self, ciphertext_b64: str) -> str:
        ciphertext_b64 = ciphertext_b64.strip()
        if ciphertext_b64.startswith("\"") and ciphertext_b64.endswith("\""):
            ciphertext_b64 = ciphertext_b64[1:-1]
        ct = base64.b64decode(ciphertext_b64)
        cipher = getattr(self, "_aes_cipher", None)
        if cipher is None:
            cipher = AES.new(self.aes_key, AES.MODE_ECB)
        pt = unpad(cipher.decrypt(ct), 16)
        return pt.decode("utf-8", "ignore")

    def _trend_cache_get(self, topic: str) -> Optional[Dict[str, object]]:
        cur = self.conn.execute(
            "SELECT first_date, last_date, duration_minutes, points FROM trend_cache_minute WHERE topic=?",
            (topic,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "first_date": row[0],
            "last_date": row[1],
            "duration_minutes": row[2],
        }

    def _trend_cache_set(self, topic: str, first_date: str, last_date: str, duration_minutes: int, points: int) -> None:
        self.conn.execute(
            """
            INSERT INTO trend_cache_minute (topic, first_date, last_date, duration_minutes, points, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic) DO UPDATE SET
                first_date=excluded.first_date,
                last_date=excluded.last_date,
                duration_minutes=excluded.duration_minutes,
                points=excluded.points,
                updated_at=excluded.updated_at
            """,
            (topic, first_date, last_date, duration_minutes, points, datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def start_requests(self) -> Iterable[scrapy.Request]:
        headers = self._build_headers()

        # Retry failed URLs from previous run (do not filter by dupefilter)
        if os.path.exists(self.failed_urls_path):
            with open(self.failed_urls_path, "r", encoding="utf-8") as f:
                for line in f:
                    url = line.strip()
                    if not url:
                        continue
                    yield scrapy.Request(
                        url,
                        headers=headers,
                        callback=self.parse_list,
                        errback=self.errback_list,
                        dont_filter=True,
                        meta={"from_failed": True},
                    )

        current = self.start_date
        while current <= self.end_date:
            chunk_end = min(current + timedelta(days=self.date_step_days - 1), self.end_date)
            yield self._make_list_request(current, chunk_end, page_no=1, headers=headers)
            current = chunk_end + timedelta(days=1)

    def _make_list_request(self, start: date, end: date, page_no: int, headers: Dict[str, str]) -> scrapy.Request:
        url = (
            f"{self.base_url}/data/list"
            f"?startDate={start.strftime('%Y-%m-%d')}"
            f"&endDate={end.strftime('%Y-%m-%d')}"
            f"&type=1"  # platform: weibo
            f"&pageNo={page_no}"
            f"&pageSize={self.page_size}"
            f"&keyword="
            f"&radioType=1"  # board: total
        )
        return scrapy.Request(
            url,
            headers=headers,
            callback=self.parse_list,
            errback=self.errback_list,
            meta={
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "page_no": page_no,
            },
        )

    def parse_list(self, response: scrapy.http.Response):
        try:
            payload = _json_loads(self._decrypt(response.text))
        except Exception as exc:
            self.logger.error("decrypt failed: %s", exc)
            return

        code = payload.get("code")
        if code != 1:
            self.logger.warning("list api error: %s", payload.get("message"))
            return

        data = payload.get("data", {}).get("data", {})
        total = int(data.get("total", 0) or 0)
        page_no = int(data.get("pageNo", response.meta.get("page_no", 1)) or 1)
        items = data.get("data", []) or []

        for row in items:
            item = self._build_item(row)
            if not self.fetch_trend:
                yield item
                continue

            topic = item.get("keyword")
            if not topic:
                yield item
                continue

            cached = self._trend_cache_get(topic)
            if cached:
                item["trend_first_time"] = cached["first_date"]
                item["trend_last_time"] = cached["last_date"]
                item["trend_duration_days"] = cached["duration_minutes"]
                yield item
                continue

            self.pending.setdefault(topic, []).append(item)
            if len(self.pending[topic]) == 1:
                headers = self._build_headers()
                if self.trend_source.lower() == "liftingdiagram":
                    url = f"{self.base_url}/data/liftingDiagram?keyword={quote(str(topic))}"
                    callback = self.parse_trend_lifting
                else:
                    url = f"{self.base_url}/data/superInfo?keyword={quote(str(topic))}"
                    callback = self.parse_trend_superinfo
                yield scrapy.Request(
                    url,
                    headers=headers,
                    callback=callback,
                    errback=self.errback_trend,
                    meta={"topic": topic, "download_timeout": self.trend_timeout},
                )

        if total > 0:
            total_pages = max(1, math.ceil(total / self.page_size))
            if page_no < total_pages:
                headers = self._build_headers()
                start = datetime.strptime(response.meta["start_date"], "%Y-%m-%d").date()
                end = datetime.strptime(response.meta["end_date"], "%Y-%m-%d").date()
                yield self._make_list_request(start, end, page_no + 1, headers)

    def parse_trend_superinfo(self, response: scrapy.http.Response):
        topic = response.meta.get("topic")
        if not topic:
            return
        try:
            payload = _json_loads(self._decrypt(response.text))
        except Exception as exc:
            self.logger.error("trend decrypt failed: %s", exc)
            pending_items = self.pending.pop(topic, [])
            for item in pending_items:
                yield item
            return

        if payload.get("code") != 1:
            self.logger.warning("trend api error for %s: %s", topic, payload.get("message"))
            pending_items = self.pending.pop(topic, [])
            for item in pending_items:
                yield item
            return

        data = payload.get("data", []) or []
        seen = set()
        first_time = None
        last_time = None
        for d in data:
            if not isinstance(d, dict):
                continue
            value = d.get("value")
            if not isinstance(value, list) or not value:
                continue
            t = str(value[0])
            seen.add(t)
            if first_time is None or t < first_time:
                first_time = t
            if last_time is None or t > last_time:
                last_time = t

        duration_minutes = len(seen)
        if first_time and last_time:
            self._trend_cache_set(topic, first_time, last_time, duration_minutes, len(seen))

        pending_items = self.pending.pop(topic, [])
        for item in pending_items:
            item["trend_first_time"] = first_time
            item["trend_last_time"] = last_time
            item["trend_duration_days"] = duration_minutes
            yield item

    def parse_trend_lifting(self, response: scrapy.http.Response):
        topic = response.meta.get("topic")
        if not topic:
            return
        try:
            payload = _json_loads(self._decrypt(response.text))
        except Exception as exc:
            self.logger.error("trend decrypt failed: %s", exc)
            pending_items = self.pending.pop(topic, [])
            for item in pending_items:
                yield item
            return

        if payload.get("code") != 1:
            self.logger.warning("trend api error for %s: %s", topic, payload.get("message"))
            pending_items = self.pending.pop(topic, [])
            for item in pending_items:
                yield item
            return

        data = payload.get("data", []) or []
        dates = set()
        first_date = None
        last_date = None
        for d in data:
            if not isinstance(d, dict):
                continue
            t = d.get("date")
            if not t:
                continue
            t = str(t)
            dates.add(t)
            if first_date is None or t < first_date:
                first_date = t
            if last_date is None or t > last_date:
                last_date = t

        duration_days = len(dates)
        if first_date and last_date:
            self._trend_cache_set(topic, first_date, last_date, duration_days, len(dates))

        pending_items = self.pending.pop(topic, [])
        for item in pending_items:
            item["trend_first_time"] = first_date
            item["trend_last_time"] = last_date
            item["trend_duration_days"] = duration_days
            yield item

    def errback_trend(self, failure):
        topic = getattr(failure.request, "meta", {}).get("topic")
        if not topic:
            return
        if failure.check(TimeoutError):
            self._record_failed_url(failure.request.url)
            raise CloseSpider("timeout_backoff")
        self.logger.warning("trend request failed for %s: %s", topic, failure.value)
        pending_items = self.pending.pop(topic, [])
        for item in pending_items:
            yield item

    def errback_list(self, failure):
        if failure.check(TimeoutError):
            self._record_failed_url(failure.request.url)
            raise CloseSpider("timeout_backoff")
        self.logger.warning("list request failed: %s", failure.value)
        self._record_failed_url(failure.request.url)

    def _record_failed_url(self, url: str) -> None:
        os.makedirs(os.path.dirname(self.failed_urls_path) or ".", exist_ok=True)
        with open(self.failed_urls_path, "a", encoding="utf-8") as f:
            f.write(url + "\n")

    def _build_item(self, row: dict) -> WeiboHotItem:
        keyword = row.get("topic") or row.get("title") or row.get("word") or row.get("name")
        last_exists = row.get("updateTime") or row.get("date") or row.get("createTime")

        item = WeiboHotItem()
        item["keyword"] = keyword
        item["rank_peak"] = row.get("pm")
        item["hot_value"] = row.get("hotNumber") or row.get("hotValue")
        item["last_exists_time"] = last_exists
        item["durations"] = row.get("durations")
        item["host_name"] = row.get("screenName")
        item["category"] = row.get("fenlei")
        item["location"] = row.get("location")
        item["icon"] = row.get("icon")
        item["trend_first_time"] = None
        item["trend_last_time"] = None
        item["trend_duration_days"] = None
        return item

    def closed(self, reason: str) -> None:
        if getattr(self, "conn", None):
            self.conn.commit()
            self.conn.close()
