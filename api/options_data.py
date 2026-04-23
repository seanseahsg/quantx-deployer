"""QuantX options data -- Cloudflare R2 parquet access with disk + memory cache.

Data layout in R2:
  s3://options-data/{SYMBOL}/greeks_1min_daily/{YYYY-MM-DD}.parquet
  s3://options-data/{SYMBOL}/index.csv      (list of available trading dates)

Local cache layout:
  {OPTIONS_CACHE_DIR}/{SYMBOL}/{YYYY-MM-DD}.parquet
  (full cleaned day, all minutes, quality-filtered)

Performance model:
  Cache miss -> boto3 download (~47MB raw) -> clean -> save (~10MB snappy).
  Cache hit  -> pd.read_parquet -> groupby time -> in-memory {HH:MM: df} dict.
  Subsequent per-minute lookups against the same day are O(1) dict lookups.
"""

from __future__ import annotations

import os
import io
import time
import logging
import threading
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable

import boto3
import pandas as pd

log = logging.getLogger("quantx-options-data")

# ── R2 connection (overridable via env vars) ─────────────────────────────────
R2_ENDPOINT = os.environ.get(
    "R2_ENDPOINT",
    "https://7f835882a6c11ee760fe4e96eb8cbef2.r2.cloudflarestorage.com",
)
R2_ACCESS_KEY = os.environ.get(
    "R2_ACCESS_KEY",
    "c51d8b873c28f70a395c02ce4b72d07d",
)
R2_SECRET_KEY = os.environ.get(
    "R2_SECRET_KEY",
    "af3f2b632698044bfb2e671954a0d30dae17c4c7e8de27803812f66ecd224b94",
)
R2_BUCKET = os.environ.get("R2_BUCKET", "options-data")
# Slim-data layout: same bucket by default, key-prefixed to avoid colliding with
# originals. A separate bucket can be used by setting R2_SLIM_BUCKET to a
# different name (requires CreateBucket permissions on the R2 key).
R2_SLIM_BUCKET = os.environ.get("R2_SLIM_BUCKET", R2_BUCKET)
R2_SLIM_PREFIX = os.environ.get("R2_SLIM_PREFIX", "slim_v1/")
# When True (default), fetch from the slim prefix -- ~215x smaller files that
# only contain the 13 minutes the backtester actually samples + DTE<=60 contracts.
# Falls back to the full path per-file if the slim object is missing,
# so a partially-populated slim layout doesn't break the app.
# Flip to False via `OPTIONS_USE_SLIM_DATA=0` for instructor mode (all minutes,
# all DTEs -- required for research queries that hit non-standard times).
USE_SLIM_DATA = os.environ.get("OPTIONS_USE_SLIM_DATA", "true").lower() in ("1", "true", "yes", "y")

# ── Cache config ─────────────────────────────────────────────────────────────
CACHE_DIR = Path(os.environ.get(
    "OPTIONS_CACHE_DIR",
    Path(__file__).parent.parent / "options_cache",
))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Number of full-day DataFrames to hold in memory (~200-600 MB each).
# Default 20 = ~4-12 GB of cache footprint, comfortable on Railway 24 GB instances.
# Boosting from 3 -> 20 means concurrent students hitting the same symbol's
# date range share warm in-memory data instead of re-reading from disk.
# Override via OPTIONS_DAY_CACHE_MAX env var.
_DAY_CACHE_MAX = int(os.environ.get("OPTIONS_DAY_CACHE_MAX", "20"))
_day_cache: "OrderedDict[tuple, dict]" = OrderedDict()
_cache_lock = threading.Lock()

# Columns the engine actually uses. R2 files have 31 cols including 18 exotic
# greeks (rho, vanna, charm, vomma, veta, vera, speed, zomma, color, ultima, d1,
# d2, dual_delta, dual_gamma, epsilon, lambda) we never reference. Keeping only
# these 13 cuts per-day cache from ~215MB to ~54MB with zstd.
_KEEP_COLS = [
    "symbol", "expiration", "strike", "right", "timestamp",
    "bid", "ask",
    "delta", "theta", "gamma", "vega",
    "implied_vol", "underlying_price",
]

# ── Boto3 client ─────────────────────────────────────────────────────────────
def get_r2_client():
    """Return a boto3 S3 client configured for Cloudflare R2.

    Timeouts: 215 MB downloads from R2 to Railway can take 30-90s; default
    boto3 read_timeout (60s) was triggering ReadTimeoutError mid-fetch.
    Adaptive retries handle transient R2 5xx without burning the whole
    backtest run.
    """
    import botocore.config
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
        config=botocore.config.Config(
            read_timeout=300,
            connect_timeout=30,
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
    )


# ── Disk cache primitives ────────────────────────────────────────────────────
def _cache_path(symbol: str, date_str: str) -> Path:
    return CACHE_DIR / symbol.upper() / f"{date_str}.parquet"


def _download_day(s3, symbol: str, date_str: str, dest: Path) -> str:
    """Download the day's parquet, preferring the slim path when enabled.
    Returns a label for the source actually used. Falls back to the full path
    on NoSuchKey so a partially-populated slim layout doesn't break requests."""
    orig_key = f"{symbol}/greeks_1min_daily/{date_str}.parquet"
    if USE_SLIM_DATA:
        slim_key = f"{R2_SLIM_PREFIX}{orig_key}"
        try:
            s3.download_file(R2_SLIM_BUCKET, slim_key, str(dest))
            return "slim"
        except Exception as e:
            msg = str(e)
            if "NoSuchKey" in msg or "404" in msg or "Not Found" in msg:
                log.info("slim miss for %s %s, falling back to full path", symbol, date_str)
            else:
                log.warning("slim download failed for %s %s: %s", symbol, date_str, e)
    s3.download_file(R2_BUCKET, orig_key, str(dest))
    return "full"


def _ensure_day_cached(symbol: str, date_str: str) -> Path:
    """Download + clean + save one day's parquet if not already cached. Thread-safe
    (filesystem-level: different processes may duplicate work but won't corrupt).

    Also validates existing files -- Railway volumes can write truncated parquets
    on disk-full. A metadata-only schema read (~1ms) catches those cleanly
    and triggers a re-download rather than letting pd.read_parquet blow up
    mid-backtest with a 'Couldn't deserialize thrift' error.
    """
    symbol = symbol.upper()
    cf = _cache_path(symbol, date_str)
    if cf.exists():
        try:
            import pyarrow.parquet as _pq
            _pq.read_schema(cf)  # fast metadata-only check
            return cf
        except Exception as e:
            log.warning("[cache] Corrupted file detected, re-downloading: %s (%s)", cf, e)
            cf.unlink(missing_ok=True)
    cf.parent.mkdir(parents=True, exist_ok=True)
    tmp = cf.with_suffix(".part")
    s3 = get_r2_client()
    log.info("[cache] Downloading %s %s from R2 to %s", symbol, date_str, cf)
    try:
        _download_day(s3, symbol, date_str, tmp)
        df = pd.read_parquet(tmp, columns=_KEEP_COLS + ["iv_error"])
        df = df[(df["iv_error"] != -1) & (df["underlying_price"] > 0)]
        df = df[_KEEP_COLS].copy()
        df.to_parquet(cf, index=False, compression="zstd")
        log.info("[cache] Saved %s %s (%.1fMB)", symbol, date_str,
                 cf.stat().st_size / 1024 / 1024)
    except Exception as e:
        log.error("[cache] FAILED to cache %s %s: %s: %s",
                  symbol, date_str, type(e).__name__, e)
        if cf.exists():
            cf.unlink(missing_ok=True)
        raise
    finally:
        tmp.unlink(missing_ok=True)
    return cf


def _hhmm_to_mod(t: str) -> int:
    """'HH:MM' -> minute-of-day int (585 for 09:45)."""
    h, m = t.split(":")
    return int(h) * 60 + int(m)


def _load_day(symbol: str, date_str: str) -> dict:
    """Return {minute_of_day:int -> DataFrame} dict for a day. Memory-cached (LRU).

    Uses integer minute-of-day keys (0-1439) instead of 'HH:MM' strings to avoid
    pandas dt.strftime, which is ~60x slower (14s -> 230ms on a 2.7M-row day).
    """
    key = (symbol.upper(), date_str)
    with _cache_lock:
        if key in _day_cache:
            _day_cache.move_to_end(key)
            return _day_cache[key]
    cf = _ensure_day_cached(symbol, date_str)
    df = pd.read_parquet(cf)
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    ns = df["timestamp"].values.astype("int64")
    mod = (ns // 60_000_000_000) % 1440
    by_mod = {int(k): g.reset_index(drop=True) for k, g in df.groupby(mod, sort=False)}
    with _cache_lock:
        _day_cache[key] = by_mod
        _day_cache.move_to_end(key)
        while len(_day_cache) > _DAY_CACHE_MAX:
            _day_cache.popitem(last=False)
    return by_mod


# ── Public: chain loading ────────────────────────────────────────────────────
def get_chain_for_date(symbol: str, date_str: str, entry_time: str = "09:45") -> pd.DataFrame:
    """Return contracts at a specific HH:MM. Quality-filtered, cached in memory + disk."""
    by_mod = _load_day(symbol, date_str)
    df = by_mod.get(_hhmm_to_mod(entry_time))
    return df if df is not None else pd.DataFrame()


# ── Public: contract selection ───────────────────────────────────────────────
def _mid(bid: float, ask: float) -> float:
    return (bid + ask) / 2.0 if bid > 0 and ask > 0 else max(bid, ask, 0.0)


def _row_to_dict(row) -> dict:
    b, a = float(row["bid"]), float(row["ask"])
    return {
        "strike": float(row["strike"]),
        "expiration": str(row["expiration"]),
        "right": str(row["right"]).upper(),
        "delta": float(row["delta"]),
        "bid": b,
        "ask": a,
        "mid": _mid(b, a),
        "implied_vol": float(row["implied_vol"]),
        "theta": float(row["theta"]),
        "gamma": float(row["gamma"]),
        "underlying_price": float(row["underlying_price"]),
    }


def find_option_by_delta(chain_df: pd.DataFrame, right: str, target_delta: float) -> dict:
    df = chain_df[chain_df["right"].str.upper() == right.upper()]
    if df.empty:
        raise ValueError(f"No {right} contracts in chain")
    idx = (df["delta"] - target_delta).abs().idxmin()
    return _row_to_dict(df.loc[idx])


def find_option_by_pct_otm(chain_df: pd.DataFrame, right: str, pct_otm: float, expiry: str) -> dict:
    df = chain_df[(chain_df["right"].str.upper() == right.upper()) &
                  (chain_df["expiration"].astype(str) == str(expiry))]
    if df.empty:
        raise ValueError(f"No {right} contracts for expiry {expiry}")
    spot = float(df["underlying_price"].iloc[0])
    target = spot * (1 - pct_otm) if right.upper() == "PUT" else spot * (1 + pct_otm)
    idx = (df["strike"] - target).abs().idxmin()
    return _row_to_dict(df.loc[idx])


def find_option_by_points(chain_df: pd.DataFrame, right: str, points_otm: float, expiry: str) -> dict:
    df = chain_df[(chain_df["right"].str.upper() == right.upper()) &
                  (chain_df["expiration"].astype(str) == str(expiry))]
    if df.empty:
        raise ValueError(f"No {right} contracts for expiry {expiry}")
    spot = float(df["underlying_price"].iloc[0])
    target = spot - points_otm if right.upper() == "PUT" else spot + points_otm
    idx = (df["strike"] - target).abs().idxmin()
    return _row_to_dict(df.loc[idx])


def get_available_expiries(chain_df: pd.DataFrame, min_dte: int = 0, max_dte: int = 60) -> list:
    if chain_df.empty:
        return []
    ref_date = pd.to_datetime(chain_df["timestamp"].iloc[0]).date()
    out = []
    for exp in sorted(chain_df["expiration"].astype(str).unique()):
        try:
            ed = datetime.strptime(exp[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        dte = (ed - ref_date).days
        if min_dte <= dte <= max_dte:
            out.append(exp)
    return out


# ── Public: exit monitoring ──────────────────────────────────────────────────
def get_price_at_time(symbol: str, date_str: str, expiry: str, strike: float,
                      right: str, time_str: str) -> dict:
    """Contract quote + delta/theta/underlying at a specific minute (cache-backed)."""
    by_mod = _load_day(symbol, date_str)
    df = by_mod.get(_hhmm_to_mod(time_str))
    if df is None or df.empty:
        raise ValueError(f"No data for {symbol} {date_str} {time_str}")
    mask = ((df["expiration"].astype(str) == str(expiry))
            & ((df["strike"] - float(strike)).abs() < 1e-6)
            & (df["right"].str.upper() == right.upper()))
    m = df[mask]
    if m.empty:
        raise ValueError(f"No quote for {symbol} {expiry} {strike}{right} at {time_str}")
    r = m.iloc[0]
    b, a = float(r["bid"]), float(r["ask"])
    return {
        "bid": b, "ask": a, "mid": _mid(b, a),
        "delta": float(r["delta"]), "theta": float(r["theta"]),
        "underlying_price": float(r["underlying_price"]),
    }


# ── Public: date index ───────────────────────────────────────────────────────
# TTL cache for dates lookups. Keeps us from hammering R2 when the same
# backtest page loads dozens of times per hour. (symbol -> (dates_list, fetched_at_epoch))
_dates_index_cache: dict = {}
_DATES_CACHE_TTL = 3600  # seconds


def _get_dates_from_index(symbol: str) -> list:
    """Read dates from {symbol}/index.csv in R2. Returns [] if missing/unreadable."""
    try:
        s3 = get_r2_client()
        obj = s3.get_object(Bucket=R2_BUCKET, Key=f"{symbol.upper()}/index.csv")
        content = obj["Body"].read().decode("utf-8")
        dates = []
        for line in content.strip().split("\n"):
            line = line.strip()
            if not line or line.lower().startswith("date"):
                continue  # skip header
            date_part = line.split(",")[0].strip()
            if len(date_part) == 10 and date_part[4] == "-":
                dates.append(date_part)
        return sorted(set(dates))
    except Exception as e:
        print(f"[options_data] {symbol}: index.csv read failed: {e}")
        return []


def _get_dates_from_listing(symbol: str) -> list:
    """List actual parquet files in {symbol}/greeks_1min_daily/ and synthesize
    dates from filenames. Slower (paginated), but always current."""
    try:
        s3 = get_r2_client()
        prefix = f"{symbol.upper()}/greeks_1min_daily/"
        dates = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                filename = obj["Key"].split("/")[-1]
                if filename.endswith(".parquet"):
                    date_str = filename[:-8]  # strip ".parquet"
                    if len(date_str) == 10 and date_str[4] == "-":
                        dates.append(date_str)
        return sorted(set(dates))
    except Exception as e:
        print(f"[options_data] {symbol}: bucket listing failed: {e}")
        return []


def get_available_dates(symbol: str) -> list:
    """Trading dates for a symbol. Tries index.csv first; if the index's last
    date is more than 7 calendar days old (or index is missing), falls back to
    listing the live parquet files. Results cached in memory for 1 hour."""
    sym = symbol.upper()
    now = time.time()
    cached = _dates_index_cache.get(sym)
    if cached and (now - cached[1]) < _DATES_CACHE_TTL:
        return cached[0]

    dates = _get_dates_from_index(sym)

    # Stale if last index date > 7 days old, or if index empty
    stale = True
    if dates:
        try:
            last_dt = datetime.strptime(dates[-1], "%Y-%m-%d")
            stale = (datetime.utcnow() - last_dt).days > 7
        except Exception:
            stale = True

    if stale or not dates:
        live_dates = _get_dates_from_listing(sym)
        if live_dates and len(live_dates) >= len(dates or []):
            dates = live_dates
            last = dates[-1] if dates else "none"
            print(f"[options_data] {sym}: index stale, using live listing "
                  f"({len(dates)} dates, last={last})")

    if dates:
        _dates_index_cache[sym] = (dates, now)
    return dates or []


def invalidate_dates_cache(symbol: Optional[str] = None) -> None:
    """Force refresh of the in-memory dates cache on next call."""
    if symbol:
        _dates_index_cache.pop(symbol.upper(), None)
    else:
        _dates_index_cache.clear()


# ── Cache management ─────────────────────────────────────────────────────────
def get_cache_stats() -> dict:
    """Return {total_files, total_size_mb, symbols: [{symbol, files, size_mb}]}."""
    total_files = 0
    total_size = 0
    symbols = []
    if CACHE_DIR.exists():
        for sd in sorted(CACHE_DIR.iterdir()):
            if not sd.is_dir():
                continue
            files = list(sd.glob("*.parquet"))
            size = sum(f.stat().st_size for f in files)
            if files:
                symbols.append({
                    "symbol": sd.name,
                    "files": len(files),
                    "size_mb": round(size / 1024 / 1024, 2),
                })
                total_files += len(files)
                total_size += size
    return {
        "total_files": total_files,
        "total_size_mb": round(total_size / 1024 / 1024, 2),
        "symbols": symbols,
        "cache_dir": str(CACHE_DIR),
    }


def clear_cache(symbol: Optional[str] = None) -> dict:
    """Delete cache files (all or per-symbol). Also clears in-memory LRU."""
    deleted = 0
    freed = 0
    with _cache_lock:
        if symbol:
            sym_up = symbol.upper()
            for k in list(_day_cache.keys()):
                if k[0] == sym_up:
                    del _day_cache[k]
        else:
            _day_cache.clear()
    if not CACHE_DIR.exists():
        return {"deleted_files": 0, "freed_mb": 0.0}
    dirs = [CACHE_DIR / symbol.upper()] if symbol else [d for d in CACHE_DIR.iterdir() if d.is_dir()]
    for d in dirs:
        if not d.is_dir():
            continue
        for f in d.glob("*.parquet"):
            try:
                freed += f.stat().st_size
                f.unlink()
                deleted += 1
            except OSError as e:
                log.warning("Failed to delete %s: %s", f, e)
        try:
            d.rmdir()
        except OSError:
            pass  # non-empty or already gone
    return {"deleted_files": deleted, "freed_mb": round(freed / 1024 / 1024, 2)}


def preload_cache(symbol: str, start_date: str, end_date: str,
                  progress_callback: Optional[Callable] = None) -> int:
    """Download + cache every trading day in [start_date, end_date]. Returns count."""
    all_dates = get_available_dates(symbol)
    dates = [d for d in all_dates if start_date <= d <= end_date]
    total = len(dates)
    for i, d in enumerate(dates, 1):
        _ensure_day_cached(symbol, d)
        if progress_callback:
            progress_callback(i, total, d)
    return total
