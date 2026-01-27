from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parents[1] / '.env'
    if _env_path.exists():
        load_dotenv(_env_path)
except Exception:
    # Optional .env support; ignore if dotenv not installed
    pass


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        return default
    return int(val)


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        return default
    return float(val)


BOT_NAME = "weibo_hot"

SPIDER_MODULES = ["weibo_hot.spiders"]
NEWSPIDER_MODULE = "weibo_hot.spiders"

ROBOTSTXT_OBEY = _env_bool("ROBOTSTXT_OBEY", False)

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
)

DEFAULT_REQUEST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Origin": "https://weibo.zhaoyizhe.com",
    "Referer": "https://weibo.zhaoyizhe.com/",
}

COOKIES_ENABLED = False

CONCURRENT_REQUESTS = _env_int("CONCURRENT_REQUESTS", 4)
DOWNLOAD_DELAY = _env_float("DOWNLOAD_DELAY", 0.8)
RANDOMIZE_DOWNLOAD_DELAY = _env_bool("RANDOMIZE_DOWNLOAD_DELAY", True)
DOWNLOAD_TIMEOUT = _env_int("DOWNLOAD_TIMEOUT", 30)
CONCURRENT_REQUESTS_PER_DOMAIN = _env_int("CONCURRENT_REQUESTS_PER_DOMAIN", 2)

AUTOTHROTTLE_ENABLED = _env_bool("AUTOTHROTTLE_ENABLED", True)
AUTOTHROTTLE_START_DELAY = _env_float("AUTOTHROTTLE_START_DELAY", 0.5)
AUTOTHROTTLE_MAX_DELAY = _env_float("AUTOTHROTTLE_MAX_DELAY", 5.0)
AUTOTHROTTLE_TARGET_CONCURRENCY = _env_float("AUTOTHROTTLE_TARGET_CONCURRENCY", 1.0)

RETRY_TIMES = _env_int("RETRY_TIMES", 5)
RETRY_HTTP_CODES = [429, 500, 502, 503, 504, 522, 524, 408]

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Output as JSON Lines for large volume
FEEDS = {
    os.getenv("OUTPUT_JSONL", "output/weibo_total_20191025_20251231.jsonl"): {
        "format": "jsonlines",
        "encoding": "utf8",
        "overwrite": _env_bool("FEED_OVERWRITE", False),
        "indent": None,
    }
}

# Custom settings
WEIBO_COOKIE = os.getenv("WEIBO_COOKIE", "")
FETCH_TREND = os.getenv("FETCH_TREND", "1") == "1"
DATE_STEP_DAYS = int(os.getenv("DATE_STEP_DAYS", "1"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))
TREND_CACHE_PATH = os.getenv("TREND_CACHE_PATH", "trend_cache.sqlite")
TREND_SOURCE = os.getenv("TREND_SOURCE", "superInfo")
TREND_TIMEOUT = _env_int("TREND_TIMEOUT", 60)
FAILED_URLS_PATH = os.getenv("FAILED_URLS_PATH", "output/failed_urls.txt")

# Disable Telnet Console (for security)
TELNETCONSOLE_ENABLED = False
