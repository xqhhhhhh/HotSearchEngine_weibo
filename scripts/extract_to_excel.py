import argparse
import json
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Convert JSONL to Excel without modification")
    parser.add_argument(
        "-i",
        "--input",
        default="output/weibo_total_20191025_20251231.jsonl",
        help="Input JSONL file",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="output/weibo_total_extract.xlsx",
        help="Output Excel file",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    rows = []
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            rows.append(obj)

    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
