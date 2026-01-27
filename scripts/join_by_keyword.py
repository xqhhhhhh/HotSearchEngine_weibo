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
    parser = argparse.ArgumentParser(description="Join list data with trend data by keyword")
    parser.add_argument("--list", default="output/list.jsonl", help="List JSONL file")
    parser.add_argument("--trend", default="output/trend.jsonl", help="Trend JSONL file")
    parser.add_argument("--out", default="output/joined.jsonl", help="Output JSONL file")
    args = parser.parse_args()

    list_path = Path(args.list)
    trend_path = Path(args.trend)
    out_path = Path(args.out)

    trends = load_trends(trend_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with list_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            key = obj.get("keyword")
            if key and key in trends:
                obj.update(trends[key])
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
