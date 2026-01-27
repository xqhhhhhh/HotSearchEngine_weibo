import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Extract unique keywords from list JSONL")
    parser.add_argument("--list", default="output/list.jsonl", help="List JSONL file")
    parser.add_argument("--out", default="output/keywords.txt", help="Output keyword file")
    args = parser.parse_args()

    list_path = Path(args.list)
    out_path = Path(args.out)
    seen = set()

    with list_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            key = obj.get("keyword")
            if not key or key in seen:
                continue
            seen.add(key)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for key in sorted(seen):
            f.write(key + "\n")


if __name__ == "__main__":
    main()
