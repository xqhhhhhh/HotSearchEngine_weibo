import argparse
import os
import subprocess
import time
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass


def main():
    parser = argparse.ArgumentParser(description="Run weibo_trend with backoff on timeout")
    parser.add_argument("--keywords", default=os.getenv("KEYWORDS_FILE", "output/keywords.txt"))
    parser.add_argument("--out", default=os.getenv("OUTPUT_JSONL", "output/trend.jsonl"))
    parser.add_argument("--jobdir", default=os.getenv("TREND_JOBDIR", "jobdir_trend"))
    args = parser.parse_args()

    backoff_schedule = [15 * 60, 30 * 60]
    attempt = 0

    while True:
        env = os.environ.copy()
        env["OUTPUT_JSONL"] = args.out

        cmd = [
            "scrapy",
            "crawl",
            "weibo_trend",
            "-a",
            f"keywords_file={args.keywords}",
            "-s",
            f"JOBDIR={args.jobdir}",
        ]

        proc = subprocess.Popen(cmd, env=env)
        proc.wait()

        # detect timeout_backoff finish
        state = Path(args.jobdir) / "spider.state"
        reason = ""
        if state.exists():
            try:
                txt = state.read_text(encoding="utf-8", errors="ignore")
                for line in txt.splitlines():
                    if "finish_reason" in line:
                        reason = line.strip()
                        break
            except Exception:
                pass

        if "timeout_backoff" not in reason:
            break

        if attempt >= len(backoff_schedule):
            break

        wait_seconds = backoff_schedule[attempt]
        attempt += 1
        print(f"timeout detected, sleeping {wait_seconds} seconds before retry...")
        time.sleep(wait_seconds)


if __name__ == "__main__":
    main()
