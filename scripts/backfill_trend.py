import argparse
import json
from pathlib import Path


def load_trends(path: Path):
    trends = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            key = obj.get("keyword")
            if not key:
                continue
            trends[key] = {
                "trend_first_time": obj.get("trend_first_time"),
                "trend_last_time": obj.get("trend_last_time"),
                "trend_duration_days": obj.get("trend_duration_days"),
            }
    return trends


def main():
    parser = argparse.ArgumentParser(description="Backfill trend fields into joined.jsonl")
    parser.add_argument("--joined", default="output/joined.jsonl", help="Joined JSONL file")
    parser.add_argument("--trend", default="output/trend_missing.jsonl", help="Trend JSONL file")
    parser.add_argument("--out", default="output/joined_filled.jsonl", help="Output JSONL file")
    args = parser.parse_args()

    joined_path = Path(args.joined)
    trend_path = Path(args.trend)
    out_path = Path(args.out)

    trends = load_trends(trend_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with joined_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            key = obj.get("keyword")
            if key in trends:
                t = trends[key]
                if obj.get("trend_first_time") in (None, ""):
                    obj["trend_first_time"] = t.get("trend_first_time")
                if obj.get("trend_last_time") in (None, ""):
                    obj["trend_last_time"] = t.get("trend_last_time")
                if obj.get("trend_duration_days") in (None, ""):
                    obj["trend_duration_days"] = t.get("trend_duration_days")
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
