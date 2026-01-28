from __future__ import annotations

import base64
import json
import os
import sqlite3
from typing import Dict, Iterable
from urllib.parse import quote

import scrapy
from scrapy.exceptions import CloseSpider
from twisted.internet.error import TimeoutError, ConnectionRefusedError
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad


class WeiboTrendSpider(scrapy.Spider):
    name = "weibo_trend"
    allowed_domains = ["hotengineapi.zhaoyizhe.com", "weibo.zhaoyizhe.com"]

    base_url = "https://hotengineapi.zhaoyizhe.com/hotEngineApi"
    aes_key = b"cce1d5a8d58249048623eb26b8b0ea53"

    def __init__(self, keywords_file: str = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.keywords_file = keywords_file or os.getenv("KEYWORDS_FILE", "output/keywords.txt")
        self._aes_cipher = AES.new(self.aes_key, AES.MODE_ECB)
        self.conn = None
        self.trend_source = "superInfo"
        self.skip_success = True

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider._init_from_settings(crawler.settings)
        return spider

    def _init_from_settings(self, settings):
        self.trend_cache_path = settings.get("TREND_CACHE_PATH", "trend_cache.sqlite")
        self.trend_source = str(settings.get("TREND_SOURCE", "superInfo")).strip()
        self.skip_success = bool(int(str(settings.get("TREND_SKIP_SUCCESS", "1")).strip()))
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

    def _build_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Origin": "https://weibo.zhaoyizhe.com",
            "Referer": "https://weibo.zhaoyizhe.com/",
            "User-Agent": self.settings.get("USER_AGENT"),
        }
        cookie = self.settings.get("WEIBO_COOKIE", "")
        if cookie:
            headers["Cookie"] = cookie
            # pass wbrsnew header if present
            for part in cookie.split(";"):
                part = part.strip()
                if part.startswith("wbrsnew="):
                    headers["wbrsnew"] = part.split("=", 1)[1]
                    break
        return headers

    def _decrypt(self, ciphertext_b64: str) -> str:
        ciphertext_b64 = ciphertext_b64.strip()
        if ciphertext_b64.startswith("\"") and ciphertext_b64.endswith("\""):
            ciphertext_b64 = ciphertext_b64[1:-1]
        ct = base64.b64decode(ciphertext_b64)
        pt = unpad(self._aes_cipher.decrypt(ct), 16)
        return pt.decode("utf-8", "ignore")

    def _trend_cache_get(self, topic: str):
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
            "points": row[3],
        }

    def _trend_cache_has_success(self, topic: str) -> bool:
        cached = self._trend_cache_get(topic)
        if not cached:
            return False
        return bool(cached.get("first_date")) and bool(cached.get("last_date"))

    def _trend_cache_set(self, topic: str, first_date: str, last_date: str, duration_minutes: int, points: int) -> None:
        self.conn.execute(
            """
            INSERT INTO trend_cache_minute (topic, first_date, last_date, duration_minutes, points, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(topic) DO UPDATE SET
                first_date=excluded.first_date,
                last_date=excluded.last_date,
                duration_minutes=excluded.duration_minutes,
                points=excluded.points,
                updated_at=excluded.updated_at
            """,
            (topic, first_date, last_date, duration_minutes, points),
        )
        self.conn.commit()

    def start_requests(self) -> Iterable[scrapy.Request]:
        headers = self._build_headers()
        with open(self.keywords_file, "r", encoding="utf-8") as f:
            for line in f:
                keyword = line.strip()
                if not keyword:
                    continue
                if self.skip_success and self._trend_cache_has_success(keyword):
                    continue
                if self.trend_source.lower() == "liftingdiagram":
                    url = f"{self.base_url}/data/liftingDiagram?keyword={quote(keyword)}"
                else:
                    url = f"{self.base_url}/data/superInfo?keyword={quote(keyword)}"
                yield scrapy.Request(
                    url,
                    headers=headers,
                    callback=self.parse_trend,
                    errback=self.errback_trend,
                    meta={"keyword": keyword},
                    dont_filter=True,
                )

    def parse_trend(self, response: scrapy.http.Response):
        keyword = response.meta.get("keyword")
        if not keyword:
            return
        try:
            payload = json.loads(self._decrypt(response.text))
        except Exception:
            return
        if payload.get("code") != 1:
            return
        data = payload.get("data", []) or []

        if self.trend_source.lower() == "liftingdiagram":
            dates = set()
            first_time = None
            last_time = None
            for d in data:
                if not isinstance(d, dict):
                    continue
                t = d.get("date")
                if not t:
                    continue
                t = str(t)
                dates.add(t)
                if first_time is None or t < first_time:
                    first_time = t
                if last_time is None or t > last_time:
                    last_time = t
            duration_value = len(dates)
        else:
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
            duration_value = len(seen)

        if first_time and last_time:
            self._trend_cache_set(keyword, first_time, last_time, duration_value, duration_value)

        yield {
            "keyword": keyword,
            "trend_first_time": first_time,
            "trend_last_time": last_time,
            "trend_duration_days": duration_value,
        }

    def errback_trend(self, failure):
        if failure.check(TimeoutError):
            raise CloseSpider("timeout_backoff")
        if failure.check(ConnectionRefusedError):
            raise CloseSpider("conn_refused_backoff")

    def closed(self, reason: str) -> None:
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
