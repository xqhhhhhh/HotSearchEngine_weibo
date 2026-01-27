import argparse
import os
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def split_ranges(start: datetime, end: datetime, shards: int):
    total_days = (end - start).days + 1
    base = total_days // shards
    rem = total_days % shards

    current = start
    for i in range(shards):
        days = base + (1 if i < rem else 0)
        if days <= 0:
            break
        shard_end = current + timedelta(days=days - 1)
        yield i + 1, current, shard_end
        current = shard_end + timedelta(days=1)


def read_finish_reason(jobdir: str) -> str:
    state = Path(jobdir) / "spider.state"
    if not state.exists():
        return ""
    try:
        txt = state.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    # spider.state is a python repr; simple parse for finish_reason
    for line in txt.splitlines():
        if "finish_reason" in line:
            return line.strip()
    return ""


def main():
    parser = argparse.ArgumentParser(description="Run scrapy in parallel shards with backoff")
    parser.add_argument("--shards", type=int, default=int(os.getenv("PARALLEL_SHARDS", "5")))
    parser.add_argument("--start", default=os.getenv("START_DATE", "2019-10-25"))
    parser.add_argument("--end", default=os.getenv("END_DATE", "2025-12-31"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    start = parse_date(args.start)
    end = parse_date(args.end)
    if start > end:
        raise SystemExit("START_DATE must be <= END_DATE")

    shards = list(split_ranges(start, end, args.shards))
    backoff_schedule = [15 * 60, 30 * 60]
    attempt = 0

    while True:
        procs = []
        shard_meta = []
        for idx, s, e in shards:
            env = os.environ.copy()
            env["START_DATE"] = s.strftime("%Y-%m-%d")
            env["END_DATE"] = e.strftime("%Y-%m-%d")
            env["OUTPUT_JSONL"] = f"output/part{idx}.jsonl"
            env["TREND_CACHE_PATH"] = f"trend_cache_part{idx}.sqlite"
            env["FAILED_URLS_PATH"] = f"output/failed_urls_part{idx}.txt"
            jobdir = f"jobdir_{idx}"

            cmd = [
                "scrapy",
                "crawl",
                "weibo_total",
                "-s",
                f"JOBDIR={jobdir}",
            ]

            print(f"[shard {idx}] {env['START_DATE']} -> {env['END_DATE']}")
            print(f"  output: {env['OUTPUT_JSONL']}")
            print(f"  jobdir: {jobdir}")

            if args.dry_run:
                continue

            proc = subprocess.Popen(cmd, env=env)
            procs.append(proc)
            shard_meta.append((idx, jobdir))

        if args.dry_run:
            return

        for p in procs:
            p.wait()

        # detect timeout_backoff finish
        timed_out = False
        for idx, jobdir in shard_meta:
            reason = read_finish_reason(jobdir)
            if "timeout_backoff" in reason:
                timed_out = True
                break

        if not timed_out:
            break

        if attempt >= len(backoff_schedule):
            break

        wait_seconds = backoff_schedule[attempt]
        attempt += 1
        print(f"timeout detected, sleeping {wait_seconds} seconds before retry...")
        time.sleep(wait_seconds)


if __name__ == "__main__":
    main()
