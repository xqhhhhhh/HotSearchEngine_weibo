import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


FIELDS = [
    "keyword",
    "rank_peak",
    "hot_value",
    "last_exists_time",
    "durations",
    "host_name",
    "host_id",
    "category",
    "location",
    "icon",
]


def parse_datetime(value: str):
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def parse_duration(value):
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def compute_first_time(last_time_str, durations):
    dt = parse_datetime(last_time_str)
    d = parse_duration(durations)
    if not dt or d is None:
        return None
    # durations is treated as minutes
    return (dt - timedelta(minutes=d)).strftime("%Y-%m-%d %H:%M")


def main():
    parser = argparse.ArgumentParser(description="Extract fields from jsonl and export to Excel")
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
            row = {k: obj.get(k) for k in FIELDS}
            row["first_exists_time"] = compute_first_time(
                obj.get("last_exists_time"), obj.get("durations")
            )
            rows.append(row)

    df = pd.DataFrame(rows, columns=FIELDS + ["first_exists_time"])

    # Aggregates per keyword
    df["_last_dt"] = pd.to_datetime(df["last_exists_time"], errors="coerce")
    df["_first_dt"] = pd.to_datetime(df["first_exists_time"], errors="coerce")
    df["_dur"] = pd.to_numeric(df["durations"], errors="coerce").fillna(0).astype(int)

    agg = (
        df.groupby("keyword", dropna=False)
        .agg(
            keyword_first_time=("_first_dt", "min"),
            keyword_last_time=("_last_dt", "max"),
            keyword_total_duration=("_dur", "sum"),
        )
        .reset_index()
    )

    # Format times as strings
    agg["keyword_first_time"] = agg["keyword_first_time"].dt.strftime("%Y-%m-%d %H:%M")
    agg["keyword_last_time"] = agg["keyword_last_time"].dt.strftime("%Y-%m-%d %H:%M")

    df = df.merge(agg, on="keyword", how="left")
    df = df.drop(columns=["_last_dt", "_first_dt", "_dur"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
