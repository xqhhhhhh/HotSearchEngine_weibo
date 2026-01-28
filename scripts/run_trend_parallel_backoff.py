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


def chunk_keywords(path: Path, shards: int):
    keywords = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    chunks = [[] for _ in range(shards)]
    for i, k in enumerate(keywords):
        chunks[i % shards].append(k)
    return chunks


def write_chunk(path: Path, keywords):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for k in keywords:
            f.write(k + "\n")


def read_finish_reason(jobdir: str) -> str:
    state = Path(jobdir) / "spider.state"
    if not state.exists():
        return ""
    try:
        txt = state.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""
    for line in txt.splitlines():
        if "finish_reason" in line:
            return line.strip()
    return ""


def main():
    parser = argparse.ArgumentParser(description="Run weibo_trend in parallel shards with backoff")
    parser.add_argument("--keywords", default=os.getenv("KEYWORDS_FILE", "output/keywords.txt"))
    parser.add_argument("--out", default=os.getenv("OUTPUT_JSONL", "output/trend.jsonl"))
    parser.add_argument("--shards", type=int, default=int(os.getenv("PARALLEL_SHARDS", "5")))
    parser.add_argument("--jobdir-prefix", default=os.getenv("TREND_JOBDIR_PREFIX", "jobdir_trend"))
    parser.add_argument("--output-prefix", default=os.getenv("TREND_OUTPUT_PREFIX", "output/trend_part"))
    parser.add_argument("--keywords-prefix", default=os.getenv("TREND_KEYWORDS_PREFIX", "output/keywords_part"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    keywords_path = Path(args.keywords)
    shards = args.shards
    chunks = chunk_keywords(keywords_path, shards)

    backoff_seconds = 60

    while True:
        procs = []
        shard_meta = []
        for i, words in enumerate(chunks, start=1):
            if not words:
                continue
            chunk_file = Path(f"{args.keywords_prefix}{i}.txt")
            write_chunk(chunk_file, words)

            env = os.environ.copy()
            env["OUTPUT_JSONL"] = f"{args.output_prefix}{i}.jsonl"
            env["TREND_CACHE_PATH"] = f"trend_cache_part{i}.sqlite"
            env["FAILED_URLS_PATH"] = f"output/failed_urls_trend_part{i}.txt"

            cmd = [
                "scrapy",
                "crawl",
                "weibo_trend",
                "-a",
                f"keywords_file={chunk_file}",
                "-s",
                f"JOBDIR={args.jobdir_prefix}_{i}",
            ]

            print(f"[trend shard {i}] keywords: {chunk_file}")
            print(f"  output: {args.output_prefix}{i}.jsonl")
            print(f"  jobdir: {args.jobdir_prefix}_{i}")

            if args.dry_run:
                continue

            proc = subprocess.Popen(cmd, env=env)
            procs.append(proc)
            shard_meta.append((i, f"{args.jobdir_prefix}_{i}"))

        if args.dry_run:
            return

        for p in procs:
            p.wait()

        timed_out = False
        for i, jobdir in shard_meta:
            reason = read_finish_reason(jobdir)
            if "timeout_backoff" in reason or "conn_refused_backoff" in reason:
                timed_out = True
                break

        if not timed_out:
            break

        print(f"timeout detected, sleeping {backoff_seconds} seconds before retry...")
        time.sleep(backoff_seconds)


if __name__ == "__main__":
    main()
