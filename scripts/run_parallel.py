import argparse
import os
import subprocess
import shutil
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


def main():
    parser = argparse.ArgumentParser(description="Run scrapy in parallel shards")
    parser.add_argument("--shards", type=int, default=int(os.getenv("PARALLEL_SHARDS", "5")))
    parser.add_argument("--start", default=os.getenv("START_DATE", "2019-10-25"))
    parser.add_argument("--end", default=os.getenv("END_DATE", "2025-12-31"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--reset-failed",
        action="store_true",
        default=os.getenv("PARALLEL_RESET_FAILED", "0") in {"1", "true", "yes", "y", "on"},
        help="Remove jobdir and output for failed shards to allow clean retry",
    )
    args = parser.parse_args()

    start = parse_date(args.start)
    end = parse_date(args.end)
    if start > end:
        raise SystemExit("START_DATE must be <= END_DATE")

    procs = []
    shard_meta = []
    for idx, s, e in split_ranges(start, end, args.shards):
        env = os.environ.copy()
        env["START_DATE"] = s.strftime("%Y-%m-%d")
        env["END_DATE"] = e.strftime("%Y-%m-%d")
        env["OUTPUT_JSONL"] = f"output/part{idx}.jsonl"
        env["TREND_CACHE_PATH"] = f"trend_cache_part{idx}.sqlite"
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
        shard_meta.append((idx, jobdir, env["OUTPUT_JSONL"]))

    if args.dry_run:
        return

    failed = []
    for p, meta in zip(procs, shard_meta):
        p.wait()
        if p.returncode != 0:
            failed.append(meta)

    if failed:
        Path("output").mkdir(parents=True, exist_ok=True)
        with open("output/failed_shards.txt", "w", encoding="utf-8") as f:
            for idx, jobdir, output in failed:
                f.write(f"{idx}\t{jobdir}\t{output}\n")

        if args.reset_failed:
            for idx, jobdir, output in failed:
                try:
                    shutil.rmtree(jobdir)
                except Exception:
                    pass
                try:
                    Path(output).unlink()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
