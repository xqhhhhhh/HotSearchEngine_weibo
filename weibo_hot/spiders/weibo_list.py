from __future__ import annotations

import base64
import json
import math
import os
import re
from datetime import date, datetime, timedelta
from typing import Dict, Iterable

import scrapy
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad


class WeiboListSpider(scrapy.Spider):
    name = "weibo_list"
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
        cookie = self.settings.get("WEIBO_COOKIE", "")
        if cookie:
            headers["Cookie"] = cookie
            m = re.search(r"(?:^|;\s*)wbrsnew=([^;]+)", cookie)
            if m:
                headers["wbrsnew"] = m.group(1)
        return headers

    def _decrypt(self, ciphertext_b64: str) -> str:
        ciphertext_b64 = ciphertext_b64.strip()
        if ciphertext_b64.startswith("\"") and ciphertext_b64.endswith("\""):
            ciphertext_b64 = ciphertext_b64[1:-1]
        ct = base64.b64decode(ciphertext_b64)
        cipher = AES.new(self.aes_key, AES.MODE_ECB)
        pt = unpad(cipher.decrypt(ct), 16)
        return pt.decode("utf-8", "ignore")

    def start_requests(self) -> Iterable[scrapy.Request]:
        headers = self._build_headers()
        current = self.start_date
        step = int(self.settings.get("DATE_STEP_DAYS", 1))
        while current <= self.end_date:
            chunk_end = min(current + timedelta(days=step - 1), self.end_date)
            yield self._make_list_request(current, chunk_end, page_no=1, headers=headers)
            current = chunk_end + timedelta(days=1)

    def _make_list_request(self, start: date, end: date, page_no: int, headers: Dict[str, str]) -> scrapy.Request:
        page_size = int(self.settings.get("PAGE_SIZE", 100))
        url = (
            f"{self.base_url}/data/list"
            f"?startDate={start.strftime('%Y-%m-%d')}"
            f"&endDate={end.strftime('%Y-%m-%d')}"
            f"&type=1"
            f"&pageNo={page_no}"
            f"&pageSize={page_size}"
            f"&keyword="
            f"&radioType=1"
        )
        return scrapy.Request(
            url,
            headers=headers,
            callback=self.parse_list,
            meta={
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "page_no": page_no,
            },
        )

    def parse_list(self, response: scrapy.http.Response):
        try:
            payload = json.loads(self._decrypt(response.text))
        except Exception:
            return

        if payload.get("code") != 1:
            return

        data = payload.get("data", {}).get("data", {})
        total = int(data.get("total", 0) or 0)
        page_no = int(data.get("pageNo", response.meta.get("page_no", 1)) or 1)
        items = data.get("data", []) or []

        for row in items:
            keyword = row.get("topic") or row.get("title") or row.get("word") or row.get("name")
            last_exists = row.get("updateTime") or row.get("date") or row.get("createTime")
            yield {
                "keyword": keyword,
                "rank_peak": row.get("pm"),
                "hot_value": row.get("hotNumber") or row.get("hotValue"),
                "last_exists_time": last_exists,
                "durations": row.get("durations"),
                "host_name": row.get("screenName"),
                "category": row.get("fenlei"),
                "location": row.get("location"),
                "icon": row.get("icon"),
            }

        if total > 0:
            total_pages = max(1, math.ceil(total / int(self.settings.get("PAGE_SIZE", 100))))
            if page_no < total_pages:
                headers = self._build_headers()
                start = datetime.strptime(response.meta["start_date"], "%Y-%m-%d").date()
                end = datetime.strptime(response.meta["end_date"], "%Y-%m-%d").date()
                yield self._make_list_request(start, end, page_no + 1, headers)
