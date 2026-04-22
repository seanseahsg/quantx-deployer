"""QuantX options data preprocessor.

Reads full-day parquet files from the source R2 bucket ("options-data"),
filters to only the 13 minutes backtests actually sample and to contracts
within 0-60 DTE, and uploads the slim result to a separate R2 bucket
("options-data-slim"). Backtest-facing columns and schema are unchanged,
so the consumer code needs no rewrite beyond a bucket switch.

Expected reduction: ~46 MB/day full -> ~1-2 MB/day slim (~25x smaller),
which cuts cold-cache download time proportionally.

CLI:
    python scripts/preprocess_options_data.py \\
        --symbols SPY,SPXW \\
        --start 2023-01-01 --end 2024-12-31 \\
        --workers 4
"""

from __future__ import annotations

import os
import sys
import argparse
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import boto3
import numpy as np
import pandas as pd

# Import config from the API module so credentials stay in one place.
sys.path.insert(0, str(Path(__file__).parent.parent))
from api.options_data import (  # noqa: E402
    R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET,
    R2_SLIM_BUCKET, R2_SLIM_PREFIX,
)

# ── Filter spec ───────────────────────────────────────────────────────────────
# Minute-of-day keys we keep. Everything the Options Studio's entry-time /
# check-exit-time presets can hit. 13 out of 390 market minutes = 3.3%.
KEEP_MINUTES = {
    555,  # 09:15 (pre-open for some filters)
    570,  # 09:30 (open)
    585,  # 09:45 (default entry)
    600,  # 10:00
    615,  # 10:15
    630,  # 10:30
    660,  # 11:00
    720,  # 12:00
    780,  # 13:00
    840,  # 14:00
    900,  # 15:00
    945,  # 15:45 (default EOD exit)
    960,  # 16:00 (close)
}
MAX_DTE = 60  # contracts expiring more than 60 days out are rarely backtested

KEEP_COLS = [
    "symbol", "expiration", "strike", "right", "timestamp",
    "bid", "ask",
    "delta", "theta", "gamma", "vega",
    "implied_vol", "underlying_price",
]

DST_BUCKET_DEFAULT = R2_SLIM_BUCKET   # same bucket as source by default
DST_PREFIX_DEFAULT = R2_SLIM_PREFIX   # key prefix for slim files (e.g. "slim_v1/")


# ── R2 clients ────────────────────────────────────────────────────────────────
def _make_client():
    """boto3 S3 client per thread. boto3 clients are thread-safe in practice
    but creating one-per-thread avoids contention on the internal connection
    pool during heavy parallel uploads."""
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


_tls = threading.local()


def _client():
    c = getattr(_tls, "client", None)
    if c is None:
        c = _make_client()
        _tls.client = c
    return c


# ── Per-day work ──────────────────────────────────────────────────────────────
def slim_dataframe(df: pd.DataFrame, file_date: str) -> pd.DataFrame:
    """Apply quality + minute + DTE filters. Returns the trimmed DataFrame."""
    # Quality filter (same as live consumer)
    df = df[(df["iv_error"] != -1) & (df["underlying_price"] > 0)]
    df = df[KEEP_COLS].copy()

    # Normalize timestamp and filter to keep minutes
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    ns = df["timestamp"].values.astype("int64")
    mod = (ns // 60_000_000_000) % 1440
    df = df[np.isin(mod, list(KEEP_MINUTES))]

    # DTE filter
    exp_dates = pd.to_datetime(df["expiration"]).dt.date
    file_dt = datetime.strptime(file_date, "%Y-%m-%d").date()
    dte = exp_dates.apply(lambda d: (d - file_dt).days)
    df = df[(dte >= 0) & (dte <= MAX_DTE)]

    return df.reset_index(drop=True)


def process_one(symbol: str, date_str: str, src_bucket: str, dst_bucket: str,
                dst_prefix: str = "", overwrite: bool = False) -> dict:
    """Slim one (symbol, date) file. Returns stats dict."""
    src_key = f"{symbol}/greeks_1min_daily/{date_str}.parquet"
    dst_key = f"{dst_prefix}{src_key}"
    s3 = _client()

    # Skip if already processed (idempotent).
    if not overwrite:
        try:
            s3.head_object(Bucket=dst_bucket, Key=dst_key)
            return {"symbol": symbol, "date": date_str, "status": "exists"}
        except Exception:
            pass  # proceed with processing

    tmp_dir = Path(tempfile.mkdtemp(prefix="qx-preproc-"))
    full_path = tmp_dir / "full.parquet"
    slim_path = tmp_dir / "slim.parquet"
    try:
        s3.download_file(src_bucket, src_key, str(full_path))
        orig_size = full_path.stat().st_size

        df = pd.read_parquet(full_path, columns=KEEP_COLS + ["iv_error"])
        orig_rows = len(df)
        slim = slim_dataframe(df, date_str)
        slim.to_parquet(slim_path, index=False, compression="zstd")
        slim_size = slim_path.stat().st_size
        slim_rows = len(slim)

        s3.upload_file(str(slim_path), dst_bucket, dst_key)

        return {
            "symbol": symbol, "date": date_str, "status": "processed",
            "orig_mb": orig_size / 1024 / 1024,
            "slim_mb": slim_size / 1024 / 1024,
            "orig_rows": orig_rows,
            "slim_rows": slim_rows,
            "ratio": orig_size / max(slim_size, 1),
        }
    except Exception as e:
        return {"symbol": symbol, "date": date_str, "status": "error", "error": str(e)}
    finally:
        for p in (full_path, slim_path):
            if p.exists():
                try: p.unlink()
                except OSError: pass
        try: tmp_dir.rmdir()
        except OSError: pass


# ── Date discovery ────────────────────────────────────────────────────────────
def list_dates(symbol: str, start: str, end: str, src_bucket: str) -> list:
    """List dates in [start, end] by scanning the source bucket's parquet keys."""
    s3 = _make_client()
    prefix = f"{symbol}/greeks_1min_daily/"
    dates = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=src_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            if fname.endswith(".parquet"):
                d = fname[:-8]
                if len(d) == 10 and d[4] == "-" and start <= d <= end:
                    dates.append(d)
    return sorted(set(dates))


# ── Orchestrator ──────────────────────────────────────────────────────────────
def run(symbols: list, start: str, end: str, workers: int,
        src_bucket: str, dst_bucket: str, dst_prefix: str, overwrite: bool) -> None:
    all_tasks = []
    for sym in symbols:
        dates = list_dates(sym, start, end, src_bucket)
        print(f"[discovery] {sym}: {len(dates)} dates in [{start}, {end}]")
        for d in dates:
            all_tasks.append((sym, d))

    if not all_tasks:
        print("Nothing to do.")
        return

    total = len(all_tasks)
    print(f"[run] Processing {total} (symbol, date) pairs with {workers} workers")
    print(f"[run] Source: {src_bucket}  ->  Dest: {dst_bucket} (prefix={dst_prefix!r})")

    start_time = time.time()
    stats = {"processed": 0, "exists": 0, "error": 0,
             "orig_mb": 0.0, "slim_mb": 0.0, "orig_rows": 0, "slim_rows": 0}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(process_one, sym, d, src_bucket, dst_bucket, dst_prefix, overwrite)
                   for sym, d in all_tasks]
        for i, f in enumerate(as_completed(futures), 1):
            r = f.result()
            status = r["status"]
            stats[status] = stats.get(status, 0) + 1
            if status == "processed":
                stats["orig_mb"] += r["orig_mb"]
                stats["slim_mb"] += r["slim_mb"]
                stats["orig_rows"] += r["orig_rows"]
                stats["slim_rows"] += r["slim_rows"]
            if i % 10 == 0 or i == total or status == "error":
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta_sec = (total - i) / rate if rate > 0 else 0
                print(f"  [{i}/{total}] {r['symbol']} {r['date']} {status}"
                      + (f"  err={r.get('error','')[:80]}" if status == "error" else "")
                      + f"  rate={rate:.1f}/s  eta={eta_sec/60:.1f}min")

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"Done in {elapsed/60:.1f} min")
    print(f"  processed: {stats['processed']}   exists: {stats['exists']}   error: {stats['error']}")
    if stats["processed"]:
        avg_orig = stats["orig_mb"] / stats["processed"]
        avg_slim = stats["slim_mb"] / stats["processed"]
        print(f"  avg orig size: {avg_orig:.1f} MB   avg slim size: {avg_slim:.2f} MB")
        print(f"  compression ratio: {avg_orig/max(avg_slim,0.001):.1f}x")
        print(f"  rows kept: {stats['slim_rows']:,} / {stats['orig_rows']:,} "
              f"({100*stats['slim_rows']/max(stats['orig_rows'],1):.1f}%)")
        print(f"  total slim data uploaded: {stats['slim_mb']/1024:.2f} GB")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--symbols", required=True,
                   help="Comma-separated (e.g. SPY,SPXW,AAPL)")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--src-bucket", default=R2_BUCKET)
    p.add_argument("--dst-bucket", default=DST_BUCKET_DEFAULT,
                   help="Defaults to same as --src-bucket (slim lives under --dst-prefix)")
    p.add_argument("--dst-prefix", default=DST_PREFIX_DEFAULT,
                   help="Key prefix for slim files, e.g. 'slim_v1/'")
    p.add_argument("--overwrite", action="store_true",
                   help="Reprocess even if slim file already exists")
    args = p.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    run(symbols, args.start, args.end, args.workers,
        args.src_bucket, args.dst_bucket, args.dst_prefix, args.overwrite)


if __name__ == "__main__":
    main()
