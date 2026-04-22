"""Measure the compression ratio the slim-data preprocessor achieves.

Samples 5 random SPY dates, downloads each full file, applies the same
filter as preprocess_options_data.py, writes a local slim parquet, and
reports sizes + row counts + an extrapolated full-job estimate.

Does NOT upload to R2 -- purely a local measurement. Safe to run without
the destination slim bucket existing.
"""

from __future__ import annotations

import sys
import random
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd  # noqa: E402
from api.options_data import (  # noqa: E402
    R2_BUCKET, get_r2_client, get_available_dates,
)
from scripts.preprocess_options_data import (  # noqa: E402
    KEEP_COLS, KEEP_MINUTES, MAX_DTE, slim_dataframe,
)

SAMPLES = 5
SYMBOL = "SPY"


def measure_one(symbol: str, date_str: str) -> dict:
    s3 = get_r2_client()
    key = f"{symbol}/greeks_1min_daily/{date_str}.parquet"

    with tempfile.TemporaryDirectory() as td:
        full = Path(td) / "full.parquet"
        slim = Path(td) / "slim.parquet"

        t0 = time.time()
        s3.download_file(R2_BUCKET, key, str(full))
        dl_sec = time.time() - t0
        orig_mb = full.stat().st_size / 1024 / 1024

        t1 = time.time()
        df = pd.read_parquet(full, columns=KEEP_COLS + ["iv_error"])
        orig_rows = len(df)
        sdf = slim_dataframe(df, date_str)
        slim_rows = len(sdf)
        sdf.to_parquet(slim, index=False, compression="zstd")
        slim_mb = slim.stat().st_size / 1024 / 1024
        process_sec = time.time() - t1

    return {
        "date": date_str,
        "orig_mb": orig_mb,
        "slim_mb": slim_mb,
        "orig_rows": orig_rows,
        "slim_rows": slim_rows,
        "download_sec": dl_sec,
        "process_sec": process_sec,
        "ratio": orig_mb / max(slim_mb, 0.001),
    }


def main():
    print(f"Sampling {SAMPLES} random {SYMBOL} dates...")
    all_dates = get_available_dates(SYMBOL)
    if not all_dates:
        print("No dates available -- check R2 connection.")
        return

    random.seed(42)
    picks = random.sample(all_dates, min(SAMPLES, len(all_dates)))
    picks = sorted(picks)
    print(f"  Picked: {picks}")
    print(f"  Filter: {len(KEEP_MINUTES)} minutes kept, 0-{MAX_DTE} DTE\n")

    results = []
    for i, d in enumerate(picks, 1):
        print(f"[{i}/{len(picks)}] {d} ...", end=" ", flush=True)
        try:
            r = measure_one(SYMBOL, d)
            results.append(r)
            print(f"{r['orig_mb']:.1f} MB -> {r['slim_mb']:.2f} MB  "
                  f"({r['ratio']:.1f}x)   "
                  f"{r['orig_rows']:,} -> {r['slim_rows']:,} rows   "
                  f"dl {r['download_sec']:.1f}s  proc {r['process_sec']:.1f}s")
        except Exception as e:
            print(f"ERROR: {e}")

    if not results:
        print("\nNo successful measurements.")
        return

    n = len(results)
    avg_orig = sum(r["orig_mb"] for r in results) / n
    avg_slim = sum(r["slim_mb"] for r in results) / n
    avg_ratio = avg_orig / max(avg_slim, 0.001)
    avg_row_keep = sum(r["slim_rows"] for r in results) / sum(r["orig_rows"] for r in results)
    avg_dl = sum(r["download_sec"] for r in results) / n
    avg_proc = sum(r["process_sec"] for r in results) / n
    avg_total_sec = avg_dl + avg_proc

    print("\n" + "=" * 72)
    print(f"RESULTS over {n} samples")
    print(f"  Original size:       {avg_orig:>8.1f} MB  (per day, avg)")
    print(f"  Slim size:           {avg_slim:>8.2f} MB")
    print(f"  Compression ratio:   {avg_ratio:>8.1f}x")
    print(f"  Rows kept:           {avg_row_keep*100:>8.2f}% of original")
    print(f"  Download time:       {avg_dl:>8.1f} s  (per day)")
    print(f"  Process time:        {avg_proc:>8.1f} s")
    print(f"  Total per day:       {avg_total_sec:>8.1f} s\n")

    # Extrapolate to a full job
    total_days = len(get_available_dates(SYMBOL))
    print(f"Extrapolation for full SPY (6 years = {total_days} days):")
    total_orig_gb = total_days * avg_orig / 1024
    total_slim_gb = total_days * avg_slim / 1024
    single_thread_hr = total_days * avg_total_sec / 3600
    four_thread_hr = single_thread_hr / 4
    print(f"  Total source data:   {total_orig_gb:>6.1f} GB")
    print(f"  Total slim data:     {total_slim_gb:>6.2f} GB "
          f"(saves {total_orig_gb - total_slim_gb:.1f} GB)")
    print(f"  Job time (1 worker): {single_thread_hr:>6.1f} hr")
    print(f"  Job time (4 workers):{four_thread_hr:>6.1f} hr")

    # Across all 16 symbols
    sym16_orig_gb = total_orig_gb * 16
    sym16_slim_gb = total_slim_gb * 16
    print(f"\nFor all 16 symbols (6 years each):")
    print(f"  Total source:        {sym16_orig_gb:>6.0f} GB")
    print(f"  Total slim:          {sym16_slim_gb:>6.0f} GB")
    print(f"  Job time (4 workers):{single_thread_hr*16/4:>6.1f} hr")


if __name__ == "__main__":
    main()
