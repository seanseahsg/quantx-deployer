"""QuantX Deployer — Backtest engine with FMP data + R2 cache."""

import os
import json
import math
import requests
from datetime import datetime

from .config import FMP_API_KEY as _FMP_CFG

FMP_KEY = _FMP_CFG or os.environ.get("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"

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

def fetch_ohlcv(symbol, timeframe="1day", limit=1260):
    bars, source = load_from_r2(symbol, timeframe)
    if bars:
        print(f"[BACKTEST] Cache hit: {symbol} ({len(bars)} bars)")
        return bars, "cache"

    if not FMP_KEY:
        raise ValueError("FMP_API_KEY not set")

    print(f"[BACKTEST] Fetching {symbol} {timeframe} from FMP...")
    if timeframe == "1day":
        url = f"{FMP_BASE}/historical-price-eod/full?symbol={symbol}&limit={limit}&apikey={FMP_KEY}"
    else:
        url = f"{FMP_BASE}/historical-chart/{timeframe}?symbol={symbol}&apikey={FMP_KEY}"

    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data or not isinstance(data, list):
        raise ValueError(f"No data for {symbol}")

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


# ── Signal generators ────────────────────────────────────────────────────────

def signals_ema_cross(bars, fast=10, slow=30):
    closes = [b["close"] for b in bars]
    fe, se = calc_ema(closes, fast), calc_ema(closes, slow)
    sigs = [None] * len(bars)
    for i in range(1, len(bars)):
        if fe[i] and se[i] and fe[i - 1] and se[i - 1]:
            if fe[i] > se[i] and fe[i - 1] <= se[i - 1]:
                sigs[i] = "buy"
            elif fe[i] < se[i] and fe[i - 1] >= se[i - 1]:
                sigs[i] = "sell"
    return sigs


def signals_turtle(bars, entry_period=20, exit_period=10):
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    closes = [b["close"] for b in bars]
    sigs = [None] * len(bars)
    for i in range(entry_period, len(bars)):
        ph = max(highs[i - entry_period:i])
        pl = min(lows[max(0, i - exit_period):i])
        if closes[i] > ph:
            sigs[i] = "buy"
        elif closes[i] < pl:
            sigs[i] = "sell"
    return sigs


def signals_rsi(bars, period=14, oversold=30, overbought=70):
    closes = [b["close"] for b in bars]
    rsi = calc_rsi(closes, period)
    return [("buy" if r and r < oversold else "sell" if r and r > overbought else None) for r in rsi]


def signals_macd(bars, fast=12, slow=26, signal_period=9):
    closes = [b["close"] for b in bars]
    fe, se = calc_ema(closes, fast), calc_ema(closes, slow)
    macd_line = [(f - s) if (f and s) else None for f, s in zip(fe, se)]
    valid = [m for m in macd_line if m is not None]
    if len(valid) < signal_period:
        return [None] * len(bars)
    sig_ema = calc_ema(valid, signal_period)
    offset = len(macd_line) - len(valid)
    hist = [None] * offset + [(m - s) if (m is not None and s is not None) else None for m, s in zip(valid, sig_ema)]
    sigs = [None] * len(bars)
    for i in range(1, len(bars)):
        if hist[i] is not None and hist[i - 1] is not None:
            if hist[i] > 0 and hist[i - 1] <= 0:
                sigs[i] = "buy"
            elif hist[i] < 0 and hist[i - 1] >= 0:
                sigs[i] = "sell"
    return sigs


# ── Backtest engine ──────────────────────────────────────────────────────────

def run_backtest(bars, strategy, params, initial_capital=10000):
    if len(bars) < 50:
        raise ValueError(f"Need 50+ bars, got {len(bars)}")

    sig_map = {"TURTLE": signals_turtle, "EMA_CROSS": signals_ema_cross, "RSI": signals_rsi, "MACD": signals_macd}
    fn = sig_map.get(strategy)
    if not fn:
        raise ValueError(f"Unknown strategy: {strategy}")
    signals = fn(bars, **{k: (int(v) if isinstance(v, float) and v == int(v) else v) for k, v in params.items()})

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
