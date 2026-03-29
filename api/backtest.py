"""QuantX Deployer — Backtest engine with FMP data + R2 cache."""

import os
import json
import math
import requests
from datetime import datetime

FMP_BASE = "https://financialmodelingprep.com/stable"


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

def _r2_key(symbol, timeframe):
    return f"{symbol.replace('.','_')}/{timeframe}/full.json"


def load_from_r2(symbol, timeframe):
    if not R2_ENDPOINT:
        return None, None
    try:
        import boto3
        r2 = boto3.client("s3", endpoint_url=R2_ENDPOINT, aws_access_key_id=R2_KEY_ID,
                          aws_secret_access_key=R2_SECRET, region_name="auto")
        obj = r2.get_object(Bucket=R2_BUCKET, Key=_r2_key(symbol, timeframe))
        data = json.loads(obj["Body"].read().decode())
        cached_at = data.get("cached_at", "")
        if cached_at:
            age = (datetime.utcnow() - datetime.fromisoformat(cached_at)).total_seconds()
            if age < 86400:
                return data.get("bars", []), "cache"
        return None, None
    except Exception:
        return None, None


def save_to_r2(symbol, timeframe, bars):
    if not R2_ENDPOINT:
        return False
    try:
        import boto3
        r2 = boto3.client("s3", endpoint_url=R2_ENDPOINT, aws_access_key_id=R2_KEY_ID,
                          aws_secret_access_key=R2_SECRET, region_name="auto")
        r2.put_object(Bucket=R2_BUCKET, Key=_r2_key(symbol, timeframe),
                       Body=json.dumps({"cached_at": datetime.utcnow().isoformat(),
                                        "symbol": symbol, "bars": bars}).encode(),
                       ContentType="application/json")
        return True
    except Exception as e:
        print(f"[BACKTEST] R2 save failed: {e}")
        return False


# ── FMP data fetch ───────────────────────────────────────────────────────────

def _fmp_symbol(symbol):
    """Convert our format to FMP: strip .US (US stocks don't need suffix), keep .HK/.SI."""
    if symbol.endswith(".US"):
        return symbol[:-3]
    return symbol


def fetch_ohlcv(symbol, timeframe="1day", limit=1260):
    bars, source = load_from_r2(symbol, timeframe)
    if bars:
        print(f"[BACKTEST] Cache hit: {symbol} ({len(bars)} bars)")
        return bars, "cache"

    fmp_key = _get_fmp_key()
    if not fmp_key:
        raise ValueError("FMP_API_KEY not set in environment")

    fmp_sym = _fmp_symbol(symbol)
    print(f"[BACKTEST] Fetching {symbol} ({fmp_sym}) {timeframe} from FMP...")
    if timeframe == "1day":
        url = f"{FMP_BASE}/historical-price-eod/full?symbol={fmp_sym}&limit={limit}&apikey={fmp_key}"
    else:
        url = f"{FMP_BASE}/historical-chart/{timeframe}?symbol={fmp_sym}&apikey={fmp_key}"

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
    "BB_GRID": signals_bb_grid, "BB_BREAKOUT": signals_bb_grid,
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

def run_backtest(bars, strategy, params, initial_capital=10000):
    if len(bars) < 50:
        raise ValueError(f"Need 50+ bars, got {len(bars)}")

    fn = STRATEGY_MAP.get(strategy.upper())
    if not fn:
        raise ValueError(f"Unknown strategy: {strategy}. Available: {list(STRATEGY_MAP.keys())}")
    # Convert float ints and filter non-signal params
    clean = {}
    skip = {"initial_capital", "capital_budget", "max_positions", "arena", "custom_tickers", "direction"}
    for k, v in params.items():
        if k in skip: continue
        if isinstance(v, float) and v == int(v): v = int(v)
        clean[k] = v
    signals = fn(bars, **clean)

    capital, position, entry_px = initial_capital, 0, 0.0
    trades, equity = [], []

    for i, (bar, sig) in enumerate(zip(bars, signals)):
        price = bar["close"]
        if sig == "buy" and position == 0:
            shares = int(capital / price)
            if shares > 0:
                position, entry_px = shares, price
                capital -= shares * price
                trades.append({"date": bar["date"], "side": "buy", "price": round(price, 4), "shares": shares, "pnl": None})
        elif sig == "sell" and position > 0:
            pnl = (price - entry_px) * position
            capital += position * price
            trades.append({"date": bar["date"], "side": "sell", "price": round(price, 4), "shares": position, "pnl": round(pnl, 2)})
            position, entry_px = 0, 0.0
        equity.append({"date": bar["date"], "value": round(capital + position * price, 2)})

    if position > 0:
        lp = bars[-1]["close"]
        pnl = (lp - entry_px) * position
        capital += position * lp
        trades.append({"date": bars[-1]["date"], "side": "sell (close)", "price": round(lp, 4), "shares": position, "pnl": round(pnl, 2)})

    final = capital
    ret = (final - initial_capital) / initial_capital * 100
    ny = len(bars) / 252
    cagr = ((final / initial_capital) ** (1 / max(ny, 0.1)) - 1) * 100
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

    return {
        "metrics": {"total_return_pct": round(ret, 2), "cagr_pct": round(cagr, 2), "sharpe_ratio": round(sharpe, 2),
                     "max_drawdown_pct": round(max_dd, 2), "win_rate_pct": round(wr, 1), "total_trades": len(sells),
                     "final_value": round(final, 2), "initial_capital": initial_capital, "bars_tested": len(bars)},
        "equity_curve": eq_s,
        "trades": trades[-20:],
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
            results.append({"params": params, "metrics": r["metrics"]})
        except Exception:
            pass
    results.sort(key=lambda x: x["metrics"]["sharpe_ratio"], reverse=True)
    return {"total_combinations": len(combos), "results": results, "best": results[0] if results else None}
