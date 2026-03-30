"""QuantX Deployer — Backtest engine with FMP data + R2 cache."""

import os
import json
import math
import requests
from datetime import datetime

try:
    import numpy as np
    import pandas as pd
    HAS_NUMPY = True
except ImportError:
    np = None
    pd = None
    HAS_NUMPY = False

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_V3 = "https://financialmodelingprep.com/api/v3"


def _get_fmp_key():
    """Read FMP key at request time, not import time."""
    key = os.environ.get("FMP_API_KEY", "")
    if not key:
        try:
            from .config import FMP_API_KEY
            key = FMP_API_KEY
        except Exception:
            pass
    return key

R2_ENDPOINT = os.environ.get("R2_ENDPOINT_URL", "")
R2_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BACKTEST_BUCKET", "backtest-data")


# ── R2 cache ─────────────────────────────────────────────────────────────────

import threading
_fetch_locks: dict = {}  # symbol/timeframe -> threading.Lock

INTRADAY_TFS = {"1min", "5min", "15min", "30min", "1hour", "4hour"}

def _cache_ttl_seconds(symbol, timeframe, bar_count=0):
    """Determine R2 cache TTL in seconds based on symbol, timeframe, and data size."""
    # SGX and HK: FMP cannot serve intraday — always use long TTL
    if symbol.endswith('.SI') or symbol.endswith('.HK'):
        return 30 * 86400  # 30 days
    # Bulk historical data (1000+ bars) — long TTL
    try:
        bc = int(bar_count or 0)
    except (TypeError, ValueError):
        bc = 0
    if bc >= 1000:
        return 30 * 86400  # 30 days
    # Intraday live-fetched — short TTL
    if timeframe in ("1min", "5min", "15min", "30min"):
        return 3600  # 1 hour
    if timeframe in ("1hour", "4hour"):
        return 14400  # 4 hours
    # Daily/weekly
    return 82800  # 23 hours


def _r2_key(symbol, timeframe):
    return f"{symbol}/{timeframe}/full.json"


def _r2_meta_key(symbol, timeframe):
    return f"{symbol}/{timeframe}/meta.json"


def _get_r2():
    if not R2_ENDPOINT:
        return None
    try:
        import boto3
        return boto3.client("s3", endpoint_url=R2_ENDPOINT, aws_access_key_id=R2_KEY_ID,
                            aws_secret_access_key=R2_SECRET, region_name="auto")
    except Exception:
        return None


def load_from_r2(symbol, timeframe):
    r2 = _get_r2()
    if not r2:
        return None, None
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=_r2_key(symbol, timeframe))
        data = json.loads(obj["Body"].read().decode())
        bars = data.get("bars", [])
        if not bars:
            return None, None
        cached_at = data.get("cached_at", "")
        ttl = _cache_ttl_seconds(symbol, timeframe, len(bars))
        if cached_at:
            try:
                ts = datetime.fromisoformat(cached_at.replace("Z", "+00:00"))
                if ts.tzinfo:
                    ts = ts.replace(tzinfo=None)
                age = (datetime.utcnow() - ts).total_seconds()
            except Exception:
                age = 0  # can't parse — treat as fresh
            if age < ttl:
                print(f"[R2] HIT: {symbol}/{timeframe} ({len(bars)} bars, age={int(age)}s, ttl={int(ttl)}s)")
                return bars, "cache"
            print(f"[R2] Stale: {symbol}/{timeframe} (age={int(age)}s, ttl={int(ttl)}s, bars={len(bars)})")
        else:
            # No timestamp — bulk upload, always accept
            print(f"[R2] HIT (no timestamp): {symbol}/{timeframe} ({len(bars)} bars)")
            return bars, "cache"
        return None, None
    except Exception as e:
        print(f"[R2] load error {symbol}/{timeframe}: {type(e).__name__}: {e}")
        return None, None


def save_to_r2(symbol, timeframe, bars):
    r2 = _get_r2()
    if not r2:
        return False
    try:
        now = datetime.utcnow().isoformat()
        # Save data
        r2.put_object(Bucket=R2_BUCKET, Key=_r2_key(symbol, timeframe),
                       Body=json.dumps({"cached_at": now, "symbol": symbol, "bars": bars}).encode(),
                       ContentType="application/json")
        # Save meta
        r2.put_object(Bucket=R2_BUCKET, Key=_r2_meta_key(symbol, timeframe),
                       Body=json.dumps({"cached_at": now, "symbol": symbol, "timeframe": timeframe,
                                        "bar_count": len(bars), "source": "fmp",
                                        "ttl_hours": _ttl_seconds(timeframe) / 3600}).encode(),
                       ContentType="application/json")
        print(f"[R2] Saved: {symbol}/{timeframe} ({len(bars)} bars)")
        return True
    except Exception as e:
        print(f"[R2] Save failed: {e}")
        return False


def r2_list_keys():
    """List all cached symbols/timeframes from R2."""
    r2 = _get_r2()
    if not r2:
        return []
    try:
        resp = r2.list_objects_v2(Bucket=R2_BUCKET, Prefix="", MaxKeys=1000)
        return [o["Key"] for o in resp.get("Contents", [])]
    except Exception:
        return []


# ── Concurrency lock (prevents cache stampede) ───────────────────────────────

def _load_r2_any(symbol, timeframe):
    """Load from R2, ignoring TTL. Used as last-resort fallback."""
    r2 = _get_r2()
    if not r2:
        return None
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=_r2_key(symbol, timeframe))
        data = json.loads(obj["Body"].read().decode())
        bars = data.get("bars", [])
        if bars:
            print(f"[R2] Fallback HIT (ignoring TTL): {symbol}/{timeframe} ({len(bars)} bars)")
            return bars
        return None
    except Exception:
        return None


def fetch_with_lock(symbol, timeframe, limit=1260):
    """Thread-safe fetch: only one FMP call per symbol/timeframe combo."""
    lock_key = f"{symbol}/{timeframe}"
    if lock_key not in _fetch_locks:
        _fetch_locks[lock_key] = threading.Lock()
    with _fetch_locks[lock_key]:
        # Re-check cache inside lock (another thread may have filled it)
        bars, source = load_from_r2(symbol, timeframe)
        if bars:
            return bars, "cache"
        # We're the only thread fetching this symbol now
        try:
            return _fetch_from_fmp(symbol, timeframe, limit)
        except Exception as e:
            # FMP failed — try R2 stale data as last resort
            print(f"[BACKTEST] FMP failed: {e}. Trying stale R2 data...")
            stale = _load_r2_any(symbol, timeframe)
            if stale:
                return stale, "cache-stale"
            raise  # re-raise if no fallback available


# ── FMP Fundamentals ─────────────────────────────────────────────────────────

def get_fundamentals(symbol, limit=5):
    """Fetch key financial metrics from FMP for fundamental strategies."""
    fmp_key = _get_fmp_key()
    if not fmp_key:
        return None
    clean = _fmp_symbol(symbol)
    endpoints = {
        "metrics": f"{FMP_V3}/key-metrics/{clean}?limit={limit}&apikey={fmp_key}",
        "ratios": f"{FMP_V3}/ratios/{clean}?limit={limit}&apikey={fmp_key}",
        "metrics_ttm": f"{FMP_V3}/key-metrics-ttm/{clean}?apikey={fmp_key}",
    }
    result = {}
    for name, url in endpoints.items():
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                result[name] = r.json()
        except Exception:
            pass
    return result if result else None


# ── Prewarm ──────────────────────────────────────────────────────────────────

PREWARM_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN", "GOOGL", "NFLX", "AMD",
    "INTC", "JPM", "BAC", "GS", "V", "MA", "WMT", "KO", "PEP", "JNJ", "PFE",
    "SPY", "QQQ", "TQQQ", "SQQQ", "SOXL", "GLD", "TLT", "IWM", "XLF", "SOXX",
    "0700.HK", "0005.HK", "0388.HK", "0941.HK", "1299.HK",
    "2318.HK", "3690.HK", "0883.HK", "0027.HK", "2800.HK",
    "D05.SI", "O39.SI", "U11.SI", "Z74.SI", "C6L.SI",
]

PREWARM_PRIORITY = ["TQQQ", "SPY", "AAPL", "MSFT", "0700.HK", "D05.SI", "QQQ", "NVDA", "TSLA", "GLD"]


def prewarm_symbol(symbol, timeframe="1day"):
    """Prewarm a single symbol. Returns status dict."""
    bars, source = load_from_r2(symbol, timeframe)
    if bars:
        return {"symbol": symbol, "status": "cached", "bars": len(bars), "source": "r2"}
    try:
        bars, source = _fetch_from_fmp(symbol, timeframe, 2520)
        return {"symbol": symbol, "status": "fetched", "bars": len(bars), "source": "fmp"}
    except Exception as e:
        return {"symbol": symbol, "status": "error", "error": str(e)[:80]}


# ── FMP data fetch ───────────────────────────────────────────────────────────

def _fmp_symbol(symbol):
    """Convert our format to FMP: strip .US (US stocks don't need suffix), keep .HK/.SI."""
    if symbol.endswith(".US"):
        return symbol[:-3]
    return symbol


def _aggregate_weekly(daily_bars):
    """Aggregate daily bars into weekly (Mon-Fri) bars."""
    if not daily_bars:
        return []
    from datetime import datetime as _dt
    weeks = {}
    for b in daily_bars:
        try:
            d = _dt.strptime(b["date"][:10], "%Y-%m-%d")
            # ISO week key
            wk = d.strftime("%G-W%V")
        except Exception:
            continue
        if wk not in weeks:
            weeks[wk] = {"date": b["date"][:10], "open": b["open"], "high": b["high"],
                         "low": b["low"], "close": b["close"], "volume": b["volume"]}
        else:
            w = weeks[wk]
            w["high"] = max(w["high"], b["high"])
            w["low"] = min(w["low"], b["low"])
            w["close"] = b["close"]
            w["volume"] += b["volume"]
    return list(weeks.values())


def fetch_ohlcv(symbol, timeframe="1day", limit=1260):
    """Public entry point — uses lock to prevent stampede."""
    if timeframe == "1week":
        daily_bars, _ = fetch_ohlcv(symbol, "1day", limit * 5)
        bars = _aggregate_weekly(daily_bars)
        save_to_r2(symbol, "1week", bars)
        return bars[:limit] if limit else bars, "live"
    bars, source = fetch_with_lock(symbol, timeframe, limit)
    if limit and len(bars) > limit:
        bars = bars[:limit]
    return bars, source


def _fetch_from_fmp(symbol, timeframe="1day", limit=1260):
    """Internal: actually calls FMP. Called inside lock."""
    fmp_key = _get_fmp_key()
    if not fmp_key:
        raise ValueError("FMP_API_KEY not set in environment")

    fmp_sym = _fmp_symbol(symbol)
    print(f"[R2 MISS] {symbol}/{timeframe} — fetching from FMP...")

    if timeframe in INTRADAY_TFS:
        # Try multiple FMP paths — v3, stable, then v3 with /full
        data = None
        for base in [FMP_V3, FMP_BASE]:
            url = f"{base}/historical-chart/{timeframe}/{fmp_sym}?apikey={fmp_key}"
            print(f"[BACKTEST] Trying: {url[:80]}...")
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    if data and isinstance(data, list) and len(data) > 0:
                        print(f"[BACKTEST] Success via {base}")
                        break
                    data = None
                else:
                    print(f"[BACKTEST] {base} returned {r.status_code}")
            except Exception as e:
                print(f"[BACKTEST] {base} error: {e}")
        if not data:
            raise ValueError(f"Intraday {timeframe} unavailable for {symbol}. HTTP 403/404 from FMP. Try 1day timeframe.")
    else:
        # 1day (default)
        url = f"{FMP_BASE}/historical-price-eod/full?symbol={fmp_sym}&limit={limit}&apikey={fmp_key}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
    if not data or not isinstance(data, list):
        raise ValueError(f"No data for {symbol} (FMP: {fmp_sym}). Check symbol is valid.")

    bars = []
    for b in data:
        bar = {"date": b.get("date", ""), "open": float(b.get("open", 0)), "high": float(b.get("high", 0)),
               "low": float(b.get("low", 0)), "close": float(b.get("close", 0)), "volume": float(b.get("volume", 0))}
        if bar["close"] > 0:
            bars.append(bar)
    bars = list(reversed(bars))
    save_to_r2(symbol, timeframe, bars)
    print(f"[BACKTEST] Fetched {len(bars)} bars for {symbol}")
    return bars, "live"


# ── Indicators ───────────────────────────────────────────────────────────────

def calc_ema(closes, period):
    if len(closes) < period:
        return [None] * len(closes)
    k = 2.0 / (period + 1)
    result = [None] * (period - 1)
    ema = sum(closes[:period]) / period
    result.append(ema)
    for p in closes[period:]:
        ema = p * k + ema * (1 - k)
        result.append(ema)
    return result


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return [None] * len(closes)
    result = [None] * period
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    ag, al = gains / period, losses / period

    def rv(ag, al):
        return 100 if al == 0 else 100 - 100 / (1 + ag / al)

    result.append(rv(ag, al))
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        if d > 0:
            ag = (ag * (period - 1) + d) / period
            al = al * (period - 1) / period
        else:
            ag = ag * (period - 1) / period
            al = (al * (period - 1) - d) / period
        result.append(rv(ag, al))
    return result


# ── Signal generators (all 14 strategies) ────────────────────────────────────

def calc_sma(closes, period):
    if len(closes) < period:
        return [None] * len(closes)
    result = [None] * (period - 1)
    for i in range(period - 1, len(closes)):
        result.append(sum(closes[i - period + 1:i + 1]) / period)
    return result


def calc_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return [None] * len(closes)
    trs = [None]
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    result = [None] * period
    atr = sum(t for t in trs[1:period + 1] if t) / period
    result.append(atr)
    for i in range(period + 1, len(closes)):
        atr = (atr * (period - 1) + (trs[i] or 0)) / period
        result.append(atr)
    return result


def signals_ema_cross(bars, fast_period=10, slow_period=30, **kw):
    closes = [b["close"] for b in bars]
    fe, se = calc_ema(closes, int(kw.get("fast", fast_period))), calc_ema(closes, int(kw.get("slow", slow_period)))
    sigs = [None] * len(bars)
    for i in range(1, len(bars)):
        if fe[i] and se[i] and fe[i - 1] and se[i - 1]:
            if fe[i] > se[i] and fe[i - 1] <= se[i - 1]: sigs[i] = "buy"
            elif fe[i] < se[i] and fe[i - 1] >= se[i - 1]: sigs[i] = "sell"
    return sigs


def signals_turtle(bars, entry_period=20, exit_period=10, **kw):
    h, l, c = [b["high"] for b in bars], [b["low"] for b in bars], [b["close"] for b in bars]
    ep, xp = int(entry_period), int(exit_period)
    sigs = [None] * len(bars)
    for i in range(ep, len(bars)):
        if c[i] > max(h[i - ep:i]): sigs[i] = "buy"
        elif i >= xp and c[i] < min(l[i - xp:i]): sigs[i] = "sell"
    return sigs


def signals_rsi(bars, rsi_period=14, oversold=30, overbought=70, **kw):
    p = int(kw.get("period", rsi_period))
    closes = [b["close"] for b in bars]
    rsi = calc_rsi(closes, p)
    return [("buy" if r and r < oversold else "sell" if r and r > overbought else None) for r in rsi]


def signals_macd(bars, fast=12, slow=26, signal_period=9, **kw):
    closes = [b["close"] for b in bars]
    fe, se = calc_ema(closes, int(fast)), calc_ema(closes, int(slow))
    ml = [(f - s) if (f and s) else None for f, s in zip(fe, se)]
    valid = [m for m in ml if m is not None]
    if len(valid) < int(signal_period): return [None] * len(bars)
    sig_e = calc_ema(valid, int(signal_period))
    off = len(ml) - len(valid)
    hist = [None] * off + [(m - s) if (m is not None and s is not None) else None for m, s in zip(valid, sig_e)]
    sigs = [None] * len(bars)
    for i in range(1, len(bars)):
        if hist[i] is not None and hist[i - 1] is not None:
            if hist[i] > 0 and hist[i - 1] <= 0: sigs[i] = "buy"
            elif hist[i] < 0 and hist[i - 1] >= 0: sigs[i] = "sell"
    return sigs


def signals_bb_grid(bars, period=20, num_std=2.0, **kw):
    closes = [b["close"] for b in bars]
    sma = calc_sma(closes, int(period))
    sigs = [None] * len(bars)
    p = int(period)
    for i in range(p, len(bars)):
        if sma[i] is None: continue
        std = (sum((closes[i - j] - sma[i]) ** 2 for j in range(p)) / p) ** 0.5
        if closes[i] < sma[i] - float(num_std) * std: sigs[i] = "buy"
        elif closes[i] > sma[i] + float(num_std) * std: sigs[i] = "sell"
    return sigs


def signals_vwap_reversion(bars, deviation_pct=0.1, **kw):
    sigs = [None] * len(bars)
    for i in range(1, len(bars)):
        w = bars[max(0, i - 20):i + 1]
        tv = sum(b["volume"] for b in w)
        if tv == 0: continue
        vwap = sum(b["close"] * b["volume"] for b in w) / tv
        dev = (bars[i]["close"] - vwap) / vwap * 100
        if dev < -float(deviation_pct): sigs[i] = "buy"
        elif dev > float(deviation_pct): sigs[i] = "sell"
    return sigs


def signals_momentum_breakout(bars, lookback=20, **kw):
    h, l, c = [b["high"] for b in bars], [b["low"] for b in bars], [b["close"] for b in bars]
    lb = int(lookback)
    sigs = [None] * len(bars)
    for i in range(lb, len(bars)):
        if c[i] > max(h[i - lb:i]): sigs[i] = "buy"
        elif c[i] < min(l[i - lb:i]): sigs[i] = "sell"
    return sigs


def signals_supertrend(bars, atr_period=10, multiplier=3.0, **kw):
    h, l, c = [b["high"] for b in bars], [b["low"] for b in bars], [b["close"] for b in bars]
    atr = calc_atr(h, l, c, int(atr_period))
    sigs = [None] * len(bars)
    trend = [1] * len(bars)
    ap = int(atr_period)
    for i in range(ap + 1, len(bars)):
        if atr[i] is None: continue
        hl2 = (h[i] + l[i]) / 2
        if c[i] > hl2 + float(multiplier) * atr[i]: trend[i] = 1
        elif c[i] < hl2 - float(multiplier) * atr[i]: trend[i] = -1
        else: trend[i] = trend[i - 1]
        if trend[i] == 1 and trend[i - 1] == -1: sigs[i] = "buy"
        elif trend[i] == -1 and trend[i - 1] == 1: sigs[i] = "sell"
    return sigs


def signals_buffett_bot(bars, trend_period=50, rsi_min=40, rsi_max=65, **kw):
    # TODO: Wire GET /api/fundamentals/{symbol} to filter entries
    # Buffett criteria: ROE > 0.15 for past 5 years, low debt, stable earnings
    closes = [b["close"] for b in bars]
    sma = calc_sma(closes, min(int(trend_period), len(closes)))
    rsi = calc_rsi(closes, 14)
    sigs = [None] * len(bars)
    tp = int(trend_period)
    for i in range(tp, len(bars)):
        if sma[i] is None or rsi[i] is None: continue
        if closes[i] > sma[i] and float(rsi_min) < rsi[i] < float(rsi_max): sigs[i] = "buy"
        elif closes[i] < sma[i]: sigs[i] = "sell"
    return sigs


def signals_graham_bot(bars, proximity_pct=20, rsi_threshold=40, **kw):
    # TODO: Wire GET /api/fundamentals/{symbol} to filter entries
    # Graham criteria: P/E < 15, P/B < 1.5, current ratio > 2
    c, l = [b["close"] for b in bars], [b["low"] for b in bars]
    rsi = calc_rsi(c, 14)
    sigs = [None] * len(bars)
    for i in range(50, len(bars)):
        if rsi[i] is None: continue
        lo = min(l[max(0, i - 252):i]) if i > 0 else l[0]
        prox = (c[i] - lo) / lo * 100 if lo > 0 else 100
        if prox <= float(proximity_pct) and rsi[i] < float(rsi_threshold): sigs[i] = "buy"
        elif rsi[i] > 60: sigs[i] = "sell"
    return sigs


def signals_livermore_bot(bars, breakout_period=20, volume_mult=1.5, **kw):
    c, h, v = [b["close"] for b in bars], [b["high"] for b in bars], [b["volume"] for b in bars]
    bp = int(breakout_period)
    sigs = [None] * len(bars)
    for i in range(bp + 1, len(bars)):
        ph = max(h[i - bp - 1:i - 1])
        av = sum(v[max(0, i - 20):i]) / min(20, i) if i > 0 else 1
        if c[i] > ph and (av > 0 and v[i] > av * float(volume_mult)): sigs[i] = "buy"
        if i >= 10 and c[i] < min(c[i - 10:i]): sigs[i] = "sell"
    return sigs


def signals_dalio_bot(bars, trend_ema=50, vwap_proximity=0.5, **kw):
    c, v = [b["close"] for b in bars], [b["volume"] for b in bars]
    ema = calc_ema(c, min(int(trend_ema), len(c)))
    sigs = [None] * len(bars)
    te = int(trend_ema)
    for i in range(te + 1, len(bars)):
        if ema[i] is None: continue
        wc, wv = c[max(0, i - 20):i + 1], v[max(0, i - 20):i + 1]
        tv = sum(wv)
        vwap = sum(a * b for a, b in zip(wc, wv)) / tv if tv > 0 else c[i]
        if c[i] > ema[i] and abs(c[i] - vwap) / vwap * 100 < float(vwap_proximity): sigs[i] = "buy"
        elif c[i] < ema[i]: sigs[i] = "sell"
    return sigs


def signals_simons_bot(bars, zscore_period=20, zscore_threshold=2.0, rsi_threshold=35, **kw):
    c = [b["close"] for b in bars]
    rsi = calc_rsi(c, 14)
    zp = int(zscore_period)
    sigs = [None] * len(bars)
    for i in range(zp + 14, len(bars)):
        if rsi[i] is None: continue
        w = c[i - zp:i]
        m = sum(w) / zp
        s = (sum((x - m) ** 2 for x in w) / zp) ** 0.5
        z = (c[i] - m) / s if s > 0 else 0
        if z < -float(zscore_threshold) and rsi[i] < float(rsi_threshold): sigs[i] = "buy"
        elif z > 0: sigs[i] = "sell"
    return sigs


def signals_soros_bot(bars, ema_fast=5, ema_mid=20, ema_slow=50, **kw):
    c = [b["close"] for b in bars]
    ef, em, es = calc_ema(c, int(ema_fast)), calc_ema(c, int(ema_mid)), calc_ema(c, int(ema_slow))
    sigs = [None] * len(bars)
    for i in range(int(ema_slow), len(bars)):
        if None in [ef[i], em[i], es[i]]: continue
        if ef[i] > em[i] > es[i]: sigs[i] = "buy"
        elif ef[i] < em[i] < es[i]: sigs[i] = "sell"
    return sigs


# ── Strategy map ─────────────────────────────────────────────────────────────

STRATEGY_MAP = {
    "TURTLE": signals_turtle, "TURTLE_TRADER": signals_turtle,
    "EMA_CROSS": signals_ema_cross,
    "RSI": signals_rsi, "RSI_MEAN_REVERSION": signals_rsi,
    "MACD": signals_macd, "MACD_MOMENTUM": signals_macd,
    "BB_GRID": signals_bb_grid, "BB_BREAKOUT": signals_bb_grid, "SYMMETRIC_GRID": signals_bb_grid,
    "VWAP_REVERSION": signals_vwap_reversion,
    "MOMENTUM_BREAKOUT": signals_momentum_breakout,
    "SUPERTREND": signals_supertrend,
    "BUFFETT_BOT": signals_buffett_bot,
    "GRAHAM_BOT": signals_graham_bot,
    "LIVERMORE_BOT": signals_livermore_bot,
    "DALIO_BOT": signals_dalio_bot,
    "SIMONS_BOT": signals_simons_bot,
    "SOROS_BOT": signals_soros_bot,
}


# ── Backtest engine ──────────────────────────────────────────────────────────

def run_backtest(bars, strategy, params, initial_capital=10000,
                 commission_pct=0.0, slippage_pct=0.0):
    if len(bars) < 50:
        raise ValueError(f"Need 50+ bars, got {len(bars)}")

    fn = STRATEGY_MAP.get(strategy.upper())
    if not fn:
        raise ValueError(f"Unknown strategy: {strategy}. Available: {list(STRATEGY_MAP.keys())}")
    # Convert float ints and filter non-signal params
    clean = {}
    skip = {"initial_capital", "capital_budget", "max_positions", "arena",
            "custom_tickers", "direction", "commission_pct", "slippage_pct"}
    for k, v in params.items():
        if k in skip: continue
        if isinstance(v, float) and v == int(v): v = int(v)
        clean[k] = v
    signals = fn(bars, **clean)

    buy_ct = sum(1 for s in signals if s == "buy")
    sell_ct = sum(1 for s in signals if s == "sell")
    print(f"[BT] {strategy} on {len(bars)} bars: {buy_ct} buys, {sell_ct} sells, comm={commission_pct}%, slip={slippage_pct}%")
    if buy_ct == 0:
        print(f"[BT] WARNING: 0 buy signals for {strategy}. First 20: {signals[:20]}")

    comm = float(commission_pct) / 100.0
    slip = float(slippage_pct) / 100.0
    # Track portfolio value simply: when flat, portfolio = cash
    # When long: portfolio = shares * current_price (all-in, no leftover cash)
    portfolio = float(initial_capital)
    pos_dir = 0       # 0=flat, 1=long, -1=short
    pos_shares = 0.0
    entry_cost = 0.0  # total cost basis (including commission)
    trades, equity = [], []

    for i, (bar, sig) in enumerate(zip(bars, signals)):
        price = bar["close"]
        if price is None or price <= 0:
            equity.append({"date": bar["date"], "value": round(max(portfolio, 0), 2)})
            continue
        # Mark-to-market before processing signals
        if pos_dir == 1:
            portfolio = pos_shares * price
        elif pos_dir == -1:
            portfolio = entry_cost + pos_shares * (entry_cost / pos_shares - price) if pos_shares > 0 else entry_cost
        # Floor at 0
        portfolio = max(portfolio, 0)

        if i + 1 >= len(bars):
            equity.append({"date": bar["date"], "value": round(portfolio, 2)})
            continue
        next_open = bars[i + 1]["open"]
        if next_open is None or next_open <= 0:
            equity.append({"date": bar["date"], "value": round(portfolio, 2)})
            continue

        # ── LONG ENTRY ──
        if sig == "buy" and pos_dir == 0 and portfolio > 0:
            fill_px = next_open * (1 + slip)
            cost_per = fill_px * (1 + comm)
            shares = int(portfolio / cost_per)
            if shares > 0:
                entry_cost = shares * cost_per
                pos_dir, pos_shares = 1, shares
                portfolio = shares * price  # immediate MTM
                trades.append({"date": bars[i+1]["date"], "side": "buy", "price": round(fill_px, 4), "shares": shares, "pnl": None})
        # ── LONG EXIT ──
        elif sig == "sell" and pos_dir == 1:
            fill_px = next_open * (1 - slip)
            proceeds = pos_shares * fill_px * (1 - comm)
            pnl = proceeds - entry_cost
            portfolio = proceeds
            trades.append({"date": bars[i+1]["date"], "side": "sell", "price": round(fill_px, 4), "shares": int(pos_shares), "pnl": round(pnl, 2)})
            pos_dir, pos_shares, entry_cost = 0, 0, 0.0
        # ── SHORT ENTRY ──
        elif sig == "short" and pos_dir == 0 and portfolio > 0:
            fill_px = next_open * (1 - slip)
            shares = int(portfolio / (fill_px * (1 + comm)))
            if shares > 0:
                entry_cost = portfolio  # remember starting equity for short
                pos_dir, pos_shares = -1, shares
                trades.append({"date": bars[i+1]["date"], "side": "short", "price": round(fill_px, 4), "shares": shares, "pnl": None})
        # ── SHORT COVER ──
        elif sig == "cover" and pos_dir == -1:
            fill_px = next_open * (1 + slip)
            cover_cost = pos_shares * fill_px * (1 + comm)
            short_proceeds = pos_shares * (entry_cost / pos_shares if pos_shares > 0 else 0)
            pnl = short_proceeds - cover_cost
            portfolio = entry_cost + pnl
            portfolio = max(portfolio, 0)
            trades.append({"date": bars[i+1]["date"], "side": "cover", "price": round(fill_px, 4), "shares": int(pos_shares), "pnl": round(pnl, 2)})
            pos_dir, pos_shares, entry_cost = 0, 0, 0.0

        equity.append({"date": bar["date"], "value": round(max(portfolio, 0), 2)})

    # Force close open positions at last price
    if pos_dir == 1 and pos_shares > 0:
        lp = bars[-1]["close"]
        proceeds = pos_shares * lp * (1 - comm)
        pnl = proceeds - entry_cost
        portfolio = proceeds
        trades.append({"date": bars[-1]["date"], "side": "sell (close)", "price": round(lp, 4), "shares": int(pos_shares), "pnl": round(pnl, 2)})
    elif pos_dir == -1 and pos_shares > 0:
        lp = bars[-1]["close"]
        cover_cost = pos_shares * lp * (1 + comm)
        pnl = (entry_cost / pos_shares * pos_shares) - cover_cost
        portfolio = entry_cost + pnl
        portfolio = max(portfolio, 0)
        trades.append({"date": bars[-1]["date"], "side": "cover (close)", "price": round(lp, 4), "shares": int(pos_shares), "pnl": round(pnl, 2)})

    final = max(portfolio, 0)
    ret = (final - initial_capital) / initial_capital * 100
    ny = len(bars) / 252
    cagr = ((final / max(initial_capital, 1)) ** (1 / max(ny, 0.1)) - 1) * 100 if final > 0 else -100
    rets = [(equity[i]["value"] - equity[i - 1]["value"]) / equity[i - 1]["value"] for i in range(1, len(equity)) if equity[i - 1]["value"] > 0]
    avg_r = sum(rets) / len(rets) if rets else 0
    std_r = (sum((r - avg_r) ** 2 for r in rets) / len(rets)) ** 0.5 if rets else 0
    sharpe = (avg_r / std_r * (252 ** 0.5)) if std_r > 0 else 0
    peak, max_dd = initial_capital, 0.0
    for p in equity:
        if p["value"] > peak:
            peak = p["value"]
        dd = (peak - p["value"]) / peak * 100
        max_dd = max(max_dd, dd)
    sells = [t for t in trades if t["pnl"] is not None]
    wins = sum(1 for t in sells if t["pnl"] > 0)
    wr = (wins / len(sells) * 100) if sells else 0

    step = max(1, len(equity) // 500)
    eq_s = equity[::step]
    if equity and eq_s[-1] != equity[-1]:
        eq_s.append(equity[-1])

    no_trades = len(sells) == 0
    NO_TRADE_HINTS = {
        "BUFFETT_BOT": "Buffett Bot works best on large-cap quality stocks (AAPL, MSFT, JNJ) on daily timeframe.",
        "GRAHAM_BOT": "Graham Bot needs beaten-down stocks near 52-week lows. Try 9988.HK or during corrections.",
        "SIMONS_BOT": "Simons Bot needs statistically extreme deviations. Try liquid ETFs (TQQQ, SPY) on 1m-5m.",
    }
    result = {
        "metrics": {"total_return_pct": round(ret, 2), "cagr_pct": round(cagr, 2), "sharpe_ratio": round(sharpe, 2),
                     "max_drawdown_pct": round(max_dd, 2), "win_rate_pct": round(wr, 1), "total_trades": len(sells),
                     "final_value": round(final, 2), "initial_capital": initial_capital, "bars_tested": len(bars)},
        "equity_curve": eq_s,
        "trades": trades[-20:],
    }
    if no_trades:
        hint = NO_TRADE_HINTS.get(strategy.upper(), "Try adjusting parameters or selecting a longer time period.")
        result["no_trades"] = True
        result["no_trades_message"] = f"No signals generated with these parameters. {hint}"
    return result


def _walk_forward_test(bars, strategy, params, initial_capital, in_sample_pct=0.7):
    """Split data 70/30. Test if OOS Sharpe >= 50% of IS Sharpe."""
    split = int(len(bars) * in_sample_pct)
    if split < 50 or len(bars) - split < 30:
        return {"pass": None, "reason": "insufficient data for split"}
    bars_is = bars[:split]
    bars_oos = bars[split:]
    try:
        r_is = run_backtest(bars_is, strategy, params, initial_capital)
        r_oos = run_backtest(bars_oos, strategy, params, initial_capital)
    except Exception:
        return {"pass": None, "reason": "backtest failed on split"}
    is_sharpe = r_is["metrics"].get("sharpe_ratio", 0) or 0
    oos_sharpe = r_oos["metrics"].get("sharpe_ratio", 0) or 0
    degradation = (is_sharpe - oos_sharpe) / max(abs(is_sharpe), 0.01)
    passes = oos_sharpe > 0 and degradation <= 0.5
    return {
        "pass": passes,
        "is_sharpe": round(is_sharpe, 2),
        "oos_sharpe": round(oos_sharpe, 2),
        "degradation_pct": round(degradation * 100, 1),
    }


def _monte_carlo_test(trades, capital, n_simulations=500, acceptable_dd=-0.30):
    """Shuffle trade sequence N times, check if P10 max DD is acceptable."""
    import random
    pnls = [t["pnl"] for t in trades if t.get("pnl") is not None]
    if len(pnls) < 5:
        return {"pass": None, "reason": "insufficient trades"}
    results = []
    for _ in range(n_simulations):
        shuffled = pnls.copy()
        random.shuffle(shuffled)
        equity = capital
        peak = capital
        max_dd = 0.0
        for pnl in shuffled:
            equity += pnl
            peak = max(peak, equity)
            dd = (equity - peak) / peak if peak > 0 else 0
            max_dd = min(max_dd, dd)
        results.append({"total_return": (equity - capital) / capital * 100, "max_dd": max_dd})
    returns = sorted(r["total_return"] for r in results)
    dds = sorted(r["max_dd"] for r in results)
    p10_ret = returns[int(n_simulations * 0.10)]
    p50_ret = returns[int(n_simulations * 0.50)]
    p90_ret = returns[int(n_simulations * 0.90)]
    p10_dd = dds[int(n_simulations * 0.10)]
    passes = p10_dd >= acceptable_dd and p10_ret > 0
    return {
        "pass": passes,
        "p10_return": round(p10_ret, 1), "p50_return": round(p50_ret, 1),
        "p90_return": round(p90_ret, 1), "p10_max_dd": round(p10_dd * 100, 1),
        "n_simulations": n_simulations,
    }


def run_optimization(bars, strategy, param_grid, initial_capital=10000):
    import itertools
    keys = list(param_grid.keys())
    combos = list(itertools.product(*param_grid.values()))
    results = []
    for combo in combos:
        params = dict(zip(keys, combo))
        try:
            r = run_backtest(bars, strategy, params, initial_capital)
            entry = {"params": params, "metrics": r["metrics"]}
            # Walk-forward validation
            wf = _walk_forward_test(bars, strategy, params, initial_capital)
            entry["walk_forward"] = wf
            # Monte Carlo simulation
            mc = _monte_carlo_test(r.get("trades", []), initial_capital)
            entry["monte_carlo"] = mc
            results.append(entry)
        except Exception:
            pass
    # Sort: walk-forward PASS first, then by Sharpe
    results.sort(key=lambda x: (
        0 if x.get("walk_forward", {}).get("pass") else 1,
        -(x["metrics"].get("sharpe_ratio", 0) or 0)
    ))
    return {"total_combinations": len(combos), "results": results, "best": results[0] if results else None}


# ── Custom Script Backtest ───────────────────────────────────────────────────

_FORBIDDEN_TOKENS = ["import ", "from ", "open(", "exec(", "eval(", "compile(",
                     "os.", "sys.", "subprocess", "shutil", "pathlib", "socket",
                     "requests.", "urllib", "http.", "__import__",
                     "globals(", "locals(", "vars(", "getattr(", "setattr(", "delattr("]


def _calc_bbands(closes, period=20, std_dev=2.0):
    """Bollinger Bands: returns (upper, mid, lower) lists."""
    sma = calc_sma(closes, period)
    upper, lower = [], []
    for i in range(len(closes)):
        if sma[i] is None:
            upper.append(None); lower.append(None)
        else:
            window = closes[max(0, i - period + 1):i + 1]
            avg = sum(window) / len(window)
            sd = (sum((x - avg) ** 2 for x in window) / len(window)) ** 0.5
            upper.append(sma[i] + std_dev * sd)
            lower.append(sma[i] - std_dev * sd)
    return upper, sma, lower


def _calc_macd(closes, fast=12, slow=26, signal=9):
    """MACD: returns (macd_line, signal_line, histogram) lists."""
    fe = calc_ema(closes, fast)
    se = calc_ema(closes, slow)
    macd_line = [None if f is None or s is None else f - s for f, s in zip(fe, se)]
    valid = [x for x in macd_line if x is not None]
    sig_line = calc_ema(valid, signal) if len(valid) >= signal else [None] * len(valid)
    # Pad signal line to match macd_line length
    pad = len(macd_line) - len(sig_line)
    sig_line = [None] * pad + sig_line
    hist = [None if m is None or s is None else m - s for m, s in zip(macd_line, sig_line)]
    return macd_line, sig_line, hist


def _calc_stoch(high_list, low_list, close_list, k_period=14, d_period=3):
    """Stochastic Oscillator. Returns (stoch_k, stoch_d) lists."""
    n = len(close_list)
    stoch_k = [None] * n
    for i in range(k_period - 1, n):
        wh = [h for h in high_list[i - k_period + 1:i + 1] if h is not None]
        wl = [l for l in low_list[i - k_period + 1:i + 1] if l is not None]
        if not wh or not wl or close_list[i] is None:
            continue
        hh, ll = max(wh), min(wl)
        stoch_k[i] = 50.0 if hh == ll else (close_list[i] - ll) / (hh - ll) * 100
    stoch_d = [None] * n
    for i in range(n):
        w = [stoch_k[j] for j in range(max(0, i - d_period + 1), i + 1) if stoch_k[j] is not None]
        if len(w) >= d_period:
            stoch_d[i] = sum(w) / len(w)
    return stoch_k, stoch_d


def _calc_adx(highs, lows, closes, period=14):
    """ADX — trend strength 0-100. Above 25 = strong trend."""
    n = len(closes)
    tr_l, pdm_l, ndm_l = [None]*n, [None]*n, [None]*n
    for i in range(1, n):
        if any(v is None for v in [highs[i], lows[i], closes[i], closes[i-1]]):
            continue
        tr_l[i] = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        up = highs[i] - highs[i-1]
        dn = lows[i-1] - lows[i]
        pdm_l[i] = up if up > dn and up > 0 else 0
        ndm_l[i] = dn if dn > up and dn > 0 else 0
    def _smooth(lst, p):
        r = [None]*n
        vals = [i for i, x in enumerate(lst) if x is not None]
        if len(vals) < p: return r
        s = vals[0]
        first_sum = sum(lst[s:s+p])
        r[s+p-1] = first_sum / p  # Wilder: initial = average, not sum
        for i in range(s+p, n):
            if lst[i] is None or r[i-1] is None: continue
            r[i] = (r[i-1] * (p-1) + lst[i]) / p  # Wilder smoothing
        return r
    atr_s, pdm_s, ndm_s = _smooth(tr_l, period), _smooth(pdm_l, period), _smooth(ndm_l, period)
    dx = [None]*n
    for i in range(n):
        if atr_s[i] is None or atr_s[i] == 0: continue
        pi = 100*pdm_s[i]/atr_s[i] if pdm_s[i] is not None else None
        ni = 100*ndm_s[i]/atr_s[i] if ndm_s[i] is not None else None
        if pi is not None and ni is not None:
            d = pi + ni
            dx[i] = 100*abs(pi-ni)/d if d != 0 else 0
    return _smooth(dx, period)


def _calc_vwap(highs, lows, closes, volumes):
    """VWAP — cumulative volume weighted average price."""
    n = len(closes)
    vwap = [None]*n
    cum_tv, cum_v = 0.0, 0.0
    for i in range(n):
        if any(v is None for v in [highs[i], lows[i], closes[i], volumes[i]]): continue
        if volumes[i] == 0: continue
        tp = (highs[i] + lows[i] + closes[i]) / 3
        cum_tv += tp * volumes[i]
        cum_v += volumes[i]
        vwap[i] = cum_tv / cum_v
    return vwap


def _calc_cci(highs, lows, closes, period=20):
    """CCI — Commodity Channel Index. Above +100 overbought, below -100 oversold."""
    n = len(closes)
    cci = [None]*n
    for i in range(period - 1, n):
        tp_w = []
        for j in range(i - period + 1, i + 1):
            if highs[j] is not None and lows[j] is not None and closes[j] is not None:
                tp_w.append((highs[j] + lows[j] + closes[j]) / 3)
        if len(tp_w) < period: continue
        tp_now = (highs[i] + lows[i] + closes[i]) / 3
        tp_mean = sum(tp_w) / len(tp_w)
        mean_dev = sum(abs(tp - tp_mean) for tp in tp_w) / len(tp_w)
        cci[i] = 0 if mean_dev == 0 else (tp_now - tp_mean) / (0.015 * mean_dev)
    return cci


def _calc_williams_r(highs, lows, closes, period=14):
    """Williams %R — momentum oscillator. Range -100 to 0."""
    n = len(closes)
    wr = [None]*n
    for i in range(period - 1, n):
        wh = [h for h in highs[i-period+1:i+1] if h is not None]
        wl = [l for l in lows[i-period+1:i+1] if l is not None]
        if not wh or not wl or closes[i] is None: continue
        hh, ll = max(wh), min(wl)
        wr[i] = -50.0 if hh == ll else (hh - closes[i]) / (hh - ll) * -100
    return wr


def _calc_donchian(highs, lows, period=20):
    """Donchian Channel — highest high and lowest low over period."""
    n = len(highs)
    upper, lower = [None]*n, [None]*n
    for i in range(period - 1, n):
        wh = [h for h in highs[i-period+1:i+1] if h is not None]
        wl = [l for l in lows[i-period+1:i+1] if l is not None]
        if wh: upper[i] = max(wh)
        if wl: lower[i] = min(wl)
    return upper, lower


def _calc_supertrend(highs, lows, closes, atr_list, period=10, multiplier=3.0):
    """Supertrend indicator. Returns (supertrend, direction) lists. direction: 1=bull, -1=bear."""
    n = len(closes)
    ub, lb = [None]*n, [None]*n
    st, dr = [None]*n, [None]*n
    for i in range(period, n):
        if atr_list[i] is None or highs[i] is None or lows[i] is None: continue
        hl2 = (highs[i] + lows[i]) / 2
        ub[i] = hl2 + multiplier * atr_list[i]
        lb[i] = hl2 - multiplier * atr_list[i]
    for i in range(period + 1, n):
        if any(v is None for v in [ub[i], lb[i], closes[i], closes[i-1]]): continue
        if ub[i-1] is not None and closes[i-1] is not None:
            if lb[i] is not None and lb[i-1] is not None:
                if closes[i-1] >= lb[i-1]: lb[i] = max(lb[i], lb[i-1])
            if ub[i] is not None and ub[i-1] is not None:
                if closes[i-1] <= ub[i-1]: ub[i] = min(ub[i], ub[i-1])
        if st[i-1] is None:
            st[i] = ub[i]; dr[i] = -1
        elif st[i-1] == ub[i-1]:
            if closes[i] <= ub[i]: st[i] = ub[i]; dr[i] = -1
            else: st[i] = lb[i]; dr[i] = 1
        else:
            if closes[i] >= lb[i]: st[i] = lb[i]; dr[i] = 1
            else: st[i] = ub[i]; dr[i] = -1
    return st, dr


def run_backtest_script(bars, script, initial_capital=10000):
    """Run a custom user script against bar data in a sandboxed exec()."""
    if not script or not script.strip():
        raise ValueError("Script is empty")
    # Security check
    for token in _FORBIDDEN_TOKENS:
        if token in script:
            raise ValueError(f"Forbidden token in script: '{token.strip()}'. Custom scripts cannot use imports or system calls.")

    # Sort bars oldest-first (standard backtest convention)
    bars = sorted(bars, key=lambda b: b.get("date", ""))

    # Build data lists
    closes = [b["close"] for b in bars]
    opens = [b["open"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    volumes = [b["volume"] for b in bars]
    dates = [b["date"] for b in bars]

    # Build a lightweight DataFrame-like dict for generate_signals(df)
    class _DFProxy:
        """Minimal DataFrame proxy — supports df['col'] and df.col access."""
        def __init__(self, data):
            self._data = data
            for k, v in data.items():
                setattr(self, k, v)
        def __getitem__(self, key):
            return self._data[key]
        def __len__(self):
            return len(next(iter(self._data.values())))
        def __contains__(self, key):
            return key in self._data

    df = _DFProxy({
        "date": dates, "open": opens, "high": highs, "low": lows,
        "close": closes, "volume": volumes,
    })

    # Sandbox globals — includes numpy/pandas for advanced scripts
    print(f"[SANDBOX] numpy={HAS_NUMPY}, np={type(np)}, pd={type(pd)}")

    sandbox = {
        "__builtins__": {
            "len": len, "range": range, "list": list, "dict": dict, "tuple": tuple,
            "set": set, "min": min, "max": max, "abs": abs, "sum": sum, "round": round,
            "zip": zip, "enumerate": enumerate, "print": print, "int": int, "float": float,
            "True": True, "False": False, "None": None, "bool": bool, "str": str,
            "sorted": sorted, "reversed": reversed, "map": map, "filter": filter,
            "isinstance": isinstance, "any": any, "all": all,
        },
        # Scientific computing (safe — no file/network access)
        "np": np, "numpy": np, "pd": pd, "pandas": pd,
        # Indicator helpers
        "calc_ema": calc_ema, "calc_sma": calc_sma, "calc_rsi": calc_rsi,
        "calc_atr": lambda period=14: calc_atr(highs, lows, closes, period),
        "calc_bbands": _calc_bbands, "calc_macd": _calc_macd,
        "calc_stoch": _calc_stoch, "calc_adx": _calc_adx,
        "calc_vwap": _calc_vwap, "calc_cci": _calc_cci,
        "calc_williams_r": _calc_williams_r, "calc_donchian": _calc_donchian,
        "calc_supertrend": _calc_supertrend,
        # Data arrays (for scripts that use raw lists instead of df)
        "opens": opens, "highs": highs, "lows": lows, "closes": closes,
        "volumes": volumes, "dates": dates, "bars": bars, "df": df,
    }

    try:
        exec(script, sandbox)
    except NameError as e:
        import re as _re
        m = _re.search(r"name '(\w+)' is not defined", str(e))
        unknown = m.group(1) if m else str(e)
        available = "calc_rsi, calc_ema, calc_sma, calc_atr, calc_bbands, calc_macd, calc_stoch, calc_adx, calc_vwap, calc_cci, calc_williams_r, calc_donchian, calc_supertrend"
        print(f"[SCRIPT] Unknown name requested: {unknown}")
        raise ValueError(f"'{unknown}' is not available. Available indicators: {available}. numpy (np) and pandas (pd) are also available.")
    except Exception as e:
        raise ValueError(f"Script error: {type(e).__name__}: {e}")

    gen_fn = sandbox.get("generate_signals")
    if not gen_fn or not callable(gen_fn):
        raise ValueError("Script must define a 'generate_signals(df)' function")

    # Always try passing df first, fall back to no-arg call
    try:
        signals = gen_fn(df)
    except TypeError as te:
        if "argument" in str(te) or "positional" in str(te):
            try:
                signals = gen_fn()
            except NameError as ne:
                import re as _re
                m = _re.search(r"name '(\w+)' is not defined", str(ne))
                unknown = m.group(1) if m else str(ne)
                raise ValueError(f"'{unknown}' is not available in the script sandbox.")
            except Exception as e2:
                raise ValueError(f"generate_signals() error: {type(e2).__name__}: {e2}")
        else:
            raise ValueError(f"generate_signals(df) error: {type(te).__name__}: {te}")
    except NameError as ne:
        import re as _re
        m = _re.search(r"name '(\w+)' is not defined", str(ne))
        unknown = m.group(1) if m else str(ne)
        raise ValueError(f"'{unknown}' is not available in the script sandbox.")
    except Exception as e:
        raise ValueError(f"generate_signals(df) error: {type(e).__name__}: {e}")

    if not isinstance(signals, (list, tuple)):
        try:
            signals = list(signals)
        except Exception:
            raise ValueError("generate_signals must return a list")
    if len(signals) != len(bars):
        raise ValueError(f"signals length ({len(signals)}) must match data length ({len(bars)})")

    # Debug: log signal counts
    sig_list = list(signals)
    buy_ct = sum(1 for s in sig_list if s == 1 or s == "buy")
    sell_ct = sum(1 for s in sig_list if s == -1 or s == "sell")
    short_ct = sum(1 for s in sig_list if s == 2 or s == "short")
    cover_ct = sum(1 for s in sig_list if s == -2 or s == "cover")
    print(f"[SCRIPT] {len(sig_list)} bars, {buy_ct} buys, {sell_ct} sells, {short_ct} shorts, {cover_ct} covers")
    print(f"[SCRIPT] Date range: {dates[0]} to {dates[-1]}")

    # Convert signals to string format
    bt_signals = []
    for s in signals:
        if s == 1 or s == "buy": bt_signals.append("buy")
        elif s == -1 or s == "sell": bt_signals.append("sell")
        elif s == 2 or s == "short": bt_signals.append("short")
        elif s == -2 or s == "cover": bt_signals.append("cover")
        else: bt_signals.append(None)

    # Run through backtest engine — portfolio-based tracking (no negative equity)
    portfolio = float(initial_capital)
    pos_dir, pos_shares, entry_cost = 0, 0.0, 0.0
    trades, equity = [], []
    for i, (bar, sig) in enumerate(zip(bars, bt_signals)):
        price = bar["close"]
        if price is None or price <= 0:
            equity.append({"date": bar["date"], "value": round(max(portfolio, 0), 2)})
            continue
        # Mark-to-market
        if pos_dir == 1:
            portfolio = pos_shares * price
        elif pos_dir == -1:
            portfolio = entry_cost + pos_shares * (entry_cost / max(pos_shares, 1) - price)
        portfolio = max(portfolio, 0)
        # Long entry
        if sig == "buy" and pos_dir == 0 and portfolio > 0:
            shares = int(portfolio / price)
            if shares > 0:
                entry_cost = shares * price
                pos_dir, pos_shares = 1, shares
                portfolio = shares * price
                trades.append({"date": bar["date"], "side": "buy", "price": round(price, 4), "shares": shares, "pnl": None})
        # Long exit
        elif sig == "sell" and pos_dir == 1:
            proceeds = pos_shares * price
            pnl = proceeds - entry_cost
            portfolio = proceeds
            trades.append({"date": bar["date"], "side": "sell", "price": round(price, 4), "shares": int(pos_shares), "pnl": round(pnl, 2)})
            pos_dir, pos_shares, entry_cost = 0, 0, 0.0
        # Short entry
        elif sig == "short" and pos_dir == 0 and portfolio > 0:
            shares = int(portfolio / price)
            if shares > 0:
                entry_cost = portfolio
                pos_dir, pos_shares = -1, shares
                trades.append({"date": bar["date"], "side": "short", "price": round(price, 4), "shares": shares, "pnl": None})
        # Short cover
        elif sig == "cover" and pos_dir == -1:
            short_entry_px = entry_cost / max(pos_shares, 1)
            pnl = pos_shares * (short_entry_px - price)
            portfolio = entry_cost + pnl
            portfolio = max(portfolio, 0)
            trades.append({"date": bar["date"], "side": "cover", "price": round(price, 4), "shares": int(pos_shares), "pnl": round(pnl, 2)})
            pos_dir, pos_shares, entry_cost = 0, 0, 0.0
        equity.append({"date": bar["date"], "value": round(max(portfolio, 0), 2)})

    # Force close
    if pos_dir == 1 and pos_shares > 0:
        lp = bars[-1]["close"]
        proceeds = pos_shares * lp
        pnl = proceeds - entry_cost
        portfolio = proceeds
        trades.append({"date": bars[-1]["date"], "side": "sell (close)", "price": round(lp, 4), "shares": int(pos_shares), "pnl": round(pnl, 2)})
    elif pos_dir == -1 and pos_shares > 0:
        lp = bars[-1]["close"]
        short_entry_px = entry_cost / max(pos_shares, 1)
        pnl = pos_shares * (short_entry_px - lp)
        portfolio = entry_cost + pnl
        portfolio = max(portfolio, 0)
        trades.append({"date": bars[-1]["date"], "side": "cover (close)", "price": round(lp, 4), "shares": int(pos_shares), "pnl": round(pnl, 2)})

    final = max(portfolio, 0)
    ret = (final - initial_capital) / initial_capital * 100
    ny = len(bars) / 252
    cagr = ((final / max(initial_capital, 1)) ** (1 / max(ny, 0.1)) - 1) * 100 if final > 0 else -100
    rets = [(equity[i]["value"] - equity[i - 1]["value"]) / equity[i - 1]["value"]
            for i in range(1, len(equity)) if equity[i - 1]["value"] > 0]
    avg_r = sum(rets) / len(rets) if rets else 0
    std_r = (sum((r - avg_r) ** 2 for r in rets) / len(rets)) ** 0.5 if rets else 0
    sharpe = (avg_r / std_r * (252 ** 0.5)) if std_r > 0 else 0
    peak, max_dd = initial_capital, 0.0
    for p in equity:
        if p["value"] > peak: peak = p["value"]
        dd = (peak - p["value"]) / peak * 100
        max_dd = max(max_dd, dd)
    sells = [t for t in trades if t["pnl"] is not None]
    wins = sum(1 for t in sells if t["pnl"] > 0)
    wr = (wins / len(sells) * 100) if sells else 0

    step = max(1, len(equity) // 500)
    eq_s = equity[::step]
    if equity and eq_s[-1] != equity[-1]:
        eq_s.append(equity[-1])

    return {
        "metrics": {"total_return_pct": round(ret, 2), "cagr_pct": round(cagr, 2), "sharpe_ratio": round(sharpe, 2),
                     "max_drawdown_pct": round(max_dd, 2), "win_rate_pct": round(wr, 1), "total_trades": len(sells),
                     "final_value": round(final, 2), "initial_capital": initial_capital, "bars_tested": len(bars)},
        "equity_curve": eq_s, "trades": trades[-50:],
    }
