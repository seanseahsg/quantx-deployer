"""QuantX options backtest engine.

Simulates vertical spreads, iron condors, strangles, and long single legs
against 1-minute greeks data in Cloudflare R2 (via api/options_data.py).

Public API:
  run_options_backtest(config)         -> {"metrics": {...}, "trade_log": [...]}
  run_options_backtest_stream(config)  -> generator yielding SSE-shaped dicts

Helpers:
  get_position_value(legs, chain_df, slip_pct=0.0) -> float | None
  get_expiry_pnl(legs, underlying_price)           -> float
  compute_atm_iv(chain_df, underlying_price, expiry=None) -> float | None
"""

from __future__ import annotations

import math
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import pandas as pd

from api import options_data

log = logging.getLogger("quantx-options-backtest")

# Process-local cache: avoid re-fetching index.csv for every trade's exit phase.
_dates_cache: dict = {}

# Thread pool for parallel disk-cache prewarm of exit dates.
# boto3.download_file is thread-safe; per-date work is independent.
_PREWARM_WORKERS = 4
_prewarm_pool = ThreadPoolExecutor(max_workers=_PREWARM_WORKERS,
                                   thread_name_prefix="options-prewarm")


def _prewarm_exit_dates(symbol: str, entry_date: str, expiry: str) -> None:
    """Download + cache all exit-date files in parallel, blocking until done.

    On warm cache this is a no-op (each _ensure_day_cached returns immediately
    when the file already exists).
    """
    all_dates = _cached_dates(symbol)
    dates = [d for d in all_dates if entry_date < d <= expiry]
    if not dates:
        return
    futures = [_prewarm_pool.submit(options_data._ensure_day_cached, symbol.upper(), d)
               for d in dates]
    for f in futures:
        try:
            f.result()
        except Exception as e:
            log.debug("prewarm failed: %s", e)


def _prewarm_all_window_dates(symbol: str, dates: list) -> None:
    """Fire-and-forget parallel download of every date in the backtest window.

    Called once at the start of a backtest run. On warm cache, each
    _ensure_day_cached short-circuits. On cold cache, this saturates bandwidth
    with parallel downloads instead of serializing them trade-by-trade.
    """
    if not dates:
        return
    for d in dates:
        _prewarm_pool.submit(options_data._ensure_day_cached, symbol.upper(), d)

_WEEKDAY_MAP = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}

_EXPIRY_REASONS = ("PROFIT_TARGET", "STOP_LOSS", "DELTA_EXIT", "DTE_EXIT", "EOD", "EXPIRED")


# ── Data helpers ─────────────────────────────────────────────────────────────
def _cached_dates(symbol: str) -> list:
    """Forward to options_data.get_available_dates, which owns its own 1-hour TTL
    cache. A process-local cache here would re-stale within the same server run."""
    return options_data.get_available_dates(symbol)


def _find_contract(chain_df: pd.DataFrame, expiration: str, strike: float, right: str):
    """Return the chain row matching an exact contract, or None."""
    mask = (
        (chain_df["expiration"].astype(str) == str(expiration))
        & (abs(chain_df["strike"] - float(strike)) < 1e-6)
        & (chain_df["right"].str.upper() == right.upper())
    )
    df = chain_df[mask]
    if df.empty:
        return None
    return df.iloc[0]


def _parse_date(s: str):
    return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()


# ── Position valuation ───────────────────────────────────────────────────────
def get_position_value(legs: list, chain_df: pd.DataFrame, slip_pct: float = 0.0) -> Optional[float]:
    """Cost to close (per share). Positive = debit to unwind. None if any leg missing."""
    total = 0.0
    for leg in legs:
        row = _find_contract(chain_df, leg["expiration"], leg["strike"], leg["right"])
        if row is None:
            return None
        bid, ask = float(row["bid"]), float(row["ask"])
        if bid <= 0 and ask <= 0:
            return None
        mid = (bid + ask) / 2.0
        slip = mid * slip_pct
        if leg["action"] == "SELL":
            total += ask + slip
        else:
            total -= max(bid - slip, 0.0)
    return total


def get_expiry_pnl(legs: list, underlying_price: float) -> float:
    """Intrinsic-value settlement cost at expiry (per share)."""
    cost = 0.0
    for leg in legs:
        if leg["right"] == "PUT":
            intrinsic = max(leg["strike"] - underlying_price, 0.0)
        else:
            intrinsic = max(underlying_price - leg["strike"], 0.0)
        if leg["action"] == "SELL":
            cost += intrinsic
        else:
            cost -= intrinsic
    return cost


def compute_atm_iv(chain_df: pd.DataFrame, underlying_price: float, expiry: Optional[str] = None) -> Optional[float]:
    """ATM IV = mean IV of the call+put at the strike closest to underlying."""
    df = chain_df
    if expiry is not None:
        df = df[df["expiration"].astype(str) == str(expiry)]
    if df.empty:
        return None
    diffs = (df["strike"] - underlying_price).abs()
    atm_strike = df.loc[diffs.idxmin(), "strike"]
    atm = df[df["strike"] == atm_strike]
    return float(atm["implied_vol"].mean()) if not atm.empty else None


# ── Strike selection & leg construction ──────────────────────────────────────
def _pick_strike_row(exp_chain: pd.DataFrame, right: str, method: str, value: float):
    """Pick one chain row for (right, expiry) matching the strike-selection method."""
    df = exp_chain[exp_chain["right"].str.upper() == right.upper()]
    if df.empty:
        return None
    spot = float(df["underlying_price"].iloc[0])
    m = method.upper()
    if m == "DELTA":
        idx = (df["delta"] - float(value)).abs().idxmin()
    elif m == "PCT_OTM":
        target = spot * (1 - float(value)) if right.upper() == "PUT" else spot * (1 + float(value))
        idx = (df["strike"] - target).abs().idxmin()
    elif m == "POINTS_OTM":
        target = spot - float(value) if right.upper() == "PUT" else spot + float(value)
        idx = (df["strike"] - target).abs().idxmin()
    else:
        raise ValueError(f"Unknown strike method: {method}")
    return df.loc[idx]


def _pick_wing_row(exp_chain: pd.DataFrame, right: str, short_row, method: str, value: float):
    """Pick the long-leg row based on wing width method."""
    df = exp_chain[exp_chain["right"].str.upper() == right.upper()]
    if df.empty:
        return None
    short_strike = float(short_row["strike"])
    m = method.upper()
    # UI sends "POINTS_OTM" / "PCT_OTM" from the wing-width dropdown; accept both
    # the short and long forms so the strike picker doesn't silently ValueError
    # and cause every trade to get skipped upstream.
    if m in ("POINTS", "POINTS_OTM"):
        target = short_strike - float(value) if right.upper() == "PUT" else short_strike + float(value)
        idx = (df["strike"] - target).abs().idxmin()
    elif m == "DELTA":
        idx = (df["delta"] - float(value)).abs().idxmin()
    elif m in ("PCT", "PCT_OTM"):
        # pct further from short strike (not from spot)
        target = short_strike * (1 - float(value)) if right.upper() == "PUT" else short_strike * (1 + float(value))
        idx = (df["strike"] - target).abs().idxmin()
    else:
        raise ValueError(f"Unknown wing method: {method}")
    row = df.loc[idx]
    if float(row["strike"]) == short_strike:
        return None  # no separate strike available
    return row


def _make_leg(row, action: str, slip_pct: float) -> dict:
    bid, ask = float(row["bid"]), float(row["ask"])
    mid = (bid + ask) / 2.0
    slip = mid * slip_pct
    if action == "SELL":
        fill = max(bid - slip, 0.0)
    else:
        fill = ask + slip
    return {
        "strike": float(row["strike"]),
        "right": str(row["right"]).upper(),
        "action": action,
        "expiration": str(row["expiration"])[:10],
        "fill_price": round(float(fill), 4),
        "delta": float(row["delta"]),
        "theta": float(row["theta"]),
        "gamma": float(row["gamma"]),
        "vega": float(row["vega"]) if "vega" in row.index else 0.0,
        "iv": float(row["implied_vol"]),
        "entry_bid": bid,
        "entry_ask": ask,
        "entry_mid": round(mid, 4),
    }


def _build_vertical(exp_chain: pd.DataFrame, right: str, short_method: str, short_value: float,
                    wing_method: str, wing_value: float, slip_pct: float) -> Optional[list]:
    short_row = _pick_strike_row(exp_chain, right, short_method, short_value)
    if short_row is None:
        return None
    long_row = _pick_wing_row(exp_chain, right, short_row, wing_method, wing_value)
    if long_row is None:
        return None
    return [_make_leg(short_row, "SELL", slip_pct), _make_leg(long_row, "BUY", slip_pct)]


def _select_legs(config: dict, chain_df: pd.DataFrame, expiry: str) -> Optional[list]:
    """Return a list of leg dicts for the strategy, or None if any strike can't be picked."""
    strategy = config["strategy_type"].upper()
    slip_pct = float(config.get("slippage_pct", 0) or 0) / 100.0
    exp_chain = chain_df[chain_df["expiration"].astype(str) == str(expiry)]
    if exp_chain.empty:
        return None

    if strategy == "SHORT_PUT_SPREAD":
        return _build_vertical(exp_chain, "PUT",
                               config["short_strike_method"], config["short_strike_value"],
                               config["wing_width_method"], config["wing_width_value"], slip_pct)

    if strategy == "SHORT_CALL_SPREAD":
        return _build_vertical(exp_chain, "CALL",
                               config["short_strike_method"], config["short_strike_value"],
                               config["wing_width_method"], config["wing_width_value"], slip_pct)

    if strategy == "IRON_CONDOR":
        put_legs = _build_vertical(exp_chain, "PUT",
                                   config["short_strike_method"], config["short_strike_value"],
                                   config["wing_width_method"], config["wing_width_value"], slip_pct)
        if put_legs is None:
            return None
        # Call side — mirror put side by default
        call_method = config.get("short_call_strike_method", config["short_strike_method"])
        call_value = config.get("short_call_strike_value")
        if call_value is None:
            call_value = abs(float(config["short_strike_value"])) if call_method.upper() == "DELTA" \
                else float(config["short_strike_value"])
        call_wing_value = config.get("call_wing_width_value", config["wing_width_value"])
        call_legs = _build_vertical(exp_chain, "CALL",
                                    call_method, call_value,
                                    config["wing_width_method"], call_wing_value, slip_pct)
        if call_legs is None:
            return None
        return put_legs + call_legs

    if strategy == "SHORT_STRANGLE":
        put_row = _pick_strike_row(exp_chain, "PUT",
                                   config["short_strike_method"], config["short_strike_value"])
        call_method = config.get("short_call_strike_method", config["short_strike_method"])
        call_value = config.get("short_call_strike_value")
        if call_value is None:
            call_value = abs(float(config["short_strike_value"])) if call_method.upper() == "DELTA" \
                else float(config["short_strike_value"])
        call_row = _pick_strike_row(exp_chain, "CALL", call_method, call_value)
        if put_row is None or call_row is None:
            return None
        return [_make_leg(put_row, "SELL", slip_pct), _make_leg(call_row, "SELL", slip_pct)]

    if strategy == "LONG_CALL":
        row = _pick_strike_row(exp_chain, "CALL",
                               config["short_strike_method"], config["short_strike_value"])
        return [_make_leg(row, "BUY", slip_pct)] if row is not None else None

    if strategy == "LONG_PUT":
        row = _pick_strike_row(exp_chain, "PUT",
                               config["short_strike_method"], config["short_strike_value"])
        return [_make_leg(row, "BUY", slip_pct)] if row is not None else None

    raise ValueError(f"Unknown strategy_type: {strategy}")


def _compute_max_loss(legs: list, net_credit: float, contracts: int) -> float:
    """Return max loss in dollars (negative for losses, -inf for undefined risk)."""
    sells = [l for l in legs if l["action"] == "SELL"]
    buys = [l for l in legs if l["action"] == "BUY"]

    # Debit-only (long call / long put)
    if not sells:
        return -abs(net_credit) * 100 * contracts

    # For each short leg, find a matching long of same right → pair as vertical
    total_width = 0.0
    paired_shorts = 0
    for s in sells:
        matches = [b for b in buys if b["right"] == s["right"]]
        if not matches:
            return -float("inf")  # naked short → undefined
        # Pair with the single matching long of same right
        b = matches[0]
        if s["right"] == "PUT":
            width = s["strike"] - b["strike"]
        else:
            width = b["strike"] - s["strike"]
        if width <= 0:
            return -float("inf")
        total_width += width
        paired_shorts += 1

    max_loss_dollars = (total_width - net_credit) * 100 * contracts
    return -max_loss_dollars


# ── Exit phase ───────────────────────────────────────────────────────────────
def _try_value_at(symbol: str, date: str, time: str, legs: list, slip_pct: float):
    """Pull chain at (date, time) and compute cost-to-close. Returns (chain_df, cost) or (None, None)."""
    try:
        chain = options_data.get_chain_for_date(symbol, date, time)
    except Exception as e:
        log.debug("chain fetch failed %s %s %s: %s", symbol, date, time, e)
        return None, None
    if chain.empty:
        return None, None
    cost = get_position_value(legs, chain, slip_pct)
    if cost is None:
        return chain, None
    return chain, cost


def _underlying_at(chain_df) -> float:
    return float(chain_df["underlying_price"].iloc[0])


def _run_exit_phase(pos: dict, config: dict) -> dict:
    """Iterate dates/times from entry to expiry, return an exit dict."""
    symbol = config["symbol"]
    entry_date = pos["entry_date"]
    entry_time = pos["entry_time"]
    expiry = pos["expiry"]
    check_times = list(config.get("check_exit_times") or [config.get("exit_time", "15:45")])
    exit_time_eod = config.get("exit_time", "15:45")
    profit_target = config.get("profit_target_pct")
    stop_loss = config.get("stop_loss_pct")
    exit_dte = config.get("exit_on_dte")
    exit_delta = config.get("exit_delta_threshold")
    slip_pct = float(config.get("slippage_pct", 0) or 0) / 100.0
    net_credit = pos["net_credit"]
    legs = pos["legs"]

    all_dates = _cached_dates(symbol)
    trade_dates = [d for d in all_dates if entry_date <= d <= expiry]
    exp_dt = _parse_date(expiry)

    for d in trade_dates:
        cur_dt = _parse_date(d)
        days_remaining = (exp_dt - cur_dt).days

        # Same-day entry: only consider times strictly after entry_time
        times_to_check = [t for t in check_times if not (d == entry_date and t <= entry_time)]

        for t in times_to_check:
            chain, cost = _try_value_at(symbol, d, t, legs, slip_pct)
            if cost is None:
                continue

            # Profit target (credit positions only)
            if profit_target is not None and net_credit > 0:
                target_cost = net_credit * (1 - float(profit_target) / 100.0)
                if cost <= target_cost:
                    return {"exit_date": d, "exit_time": t, "cost_to_close": cost,
                            "exit_reason": "PROFIT_TARGET",
                            "underlying_at_exit": _underlying_at(chain)}

            # Stop loss (credit positions only)
            if stop_loss is not None and net_credit > 0:
                loss_cost = net_credit * (1 + float(stop_loss) / 100.0)
                if cost >= loss_cost:
                    return {"exit_date": d, "exit_time": t, "cost_to_close": cost,
                            "exit_reason": "STOP_LOSS",
                            "underlying_at_exit": _underlying_at(chain)}

            # Delta-based exit on the short leg(s)
            if exit_delta is not None:
                threshold = float(exit_delta)
                for leg in legs:
                    if leg["action"] != "SELL":
                        continue
                    row = _find_contract(chain, leg["expiration"], leg["strike"], leg["right"])
                    if row is not None and abs(float(row["delta"])) >= threshold:
                        return {"exit_date": d, "exit_time": t, "cost_to_close": cost,
                                "exit_reason": "DELTA_EXIT",
                                "underlying_at_exit": _underlying_at(chain)}

            # DTE-based exit
            if exit_dte is not None and days_remaining <= int(exit_dte) and d != entry_date:
                return {"exit_date": d, "exit_time": t, "cost_to_close": cost,
                        "exit_reason": "DTE_EXIT",
                        "underlying_at_exit": _underlying_at(chain)}

            # EOD on expiry day
            if d == expiry and t == exit_time_eod:
                return {"exit_date": d, "exit_time": t, "cost_to_close": cost,
                        "exit_reason": "EOD",
                        "underlying_at_exit": _underlying_at(chain)}

        # Expiry day reached, no exit trigger — settle intrinsic
        if d == expiry:
            # Try to find a late underlying price from any check_time
            spot = None
            for t in reversed(check_times):
                try:
                    ch = options_data.get_chain_for_date(symbol, d, t)
                    if not ch.empty:
                        spot = _underlying_at(ch)
                        break
                except Exception:
                    continue
            if spot is None:
                spot = pos["underlying_at_entry"]
            cost = get_expiry_pnl(legs, spot)
            return {"exit_date": d, "exit_time": "16:00", "cost_to_close": cost,
                    "exit_reason": "EXPIRED", "underlying_at_exit": spot}

    # Fallback — no trading dates found between entry and expiry (shouldn't happen normally)
    return {"exit_date": expiry, "exit_time": "16:00", "cost_to_close": 0.0,
            "exit_reason": "EXPIRED", "underlying_at_exit": pos["underlying_at_entry"]}


# ── Trade finalization ───────────────────────────────────────────────────────
def _finalize_trade(pos: dict, exit_result: dict, config: dict) -> dict:
    contracts = int(config.get("contracts", 1) or 1)
    comm_per = float(config.get("commission_per_contract", 0) or 0)
    net_credit = pos["net_credit"]
    cost_to_close = float(exit_result["cost_to_close"])
    pnl_per_share = net_credit - cost_to_close
    pnl_gross = pnl_per_share * 100 * contracts
    total_commission = comm_per * len(pos["legs"]) * contracts * 2  # entry + exit
    pnl_net = pnl_gross - total_commission

    days_in_trade = (_parse_date(exit_result["exit_date"]) - _parse_date(pos["entry_date"])).days

    # Net greeks: BUY legs add, SELL legs subtract
    def _net(key):
        return sum(l.get(key, 0.0) * (1 if l["action"] == "BUY" else -1) for l in pos["legs"])
    net_delta = _net("delta")
    net_theta = _net("theta")
    net_gamma = _net("gamma")
    net_vega = _net("vega")

    max_profit = pos["max_profit"]
    pnl_pct_of_max = (pnl_net / max_profit * 100.0) if max_profit > 0 else 0.0

    return {
        "entry_date": pos["entry_date"],
        "entry_time": pos["entry_time"],
        "exit_date": exit_result["exit_date"],
        "exit_time": exit_result["exit_time"],
        "symbol": config["symbol"],
        "strategy_type": config["strategy_type"],
        "expiration": pos["expiry"],
        "dte_at_entry": pos["dte_at_entry"],
        "days_in_trade": days_in_trade,
        "legs": pos["legs"],
        "underlying_at_entry": round(pos["underlying_at_entry"], 4),
        "underlying_at_exit": round(float(exit_result.get("underlying_at_exit") or 0), 4),
        "net_credit": round(net_credit, 4),
        "cost_to_close": round(cost_to_close, 4),
        "pnl_gross": round(pnl_gross, 2),
        "commission": round(total_commission, 2),
        "pnl_net": round(pnl_net, 2),
        "exit_reason": exit_result["exit_reason"],
        "greeks_at_entry": {
            "net_delta": round(net_delta, 4),
            "net_theta": round(net_theta, 4),
            "net_gamma": round(net_gamma, 4),
            "net_vega": round(net_vega, 4),
            "atm_iv": round(float(pos.get("atm_iv_entry") or 0), 4),
        },
        "max_profit": round(max_profit, 2),
        "max_loss": pos["max_loss"] if not math.isinf(pos["max_loss"]) else None,
        "pnl_pct_of_max": round(pnl_pct_of_max, 1),
    }


# ── Metrics ──────────────────────────────────────────────────────────────────
def _compute_metrics(trades: list, config: dict, skipped: dict, equity: list) -> dict:
    n = len(trades)
    base = {f"skipped_{k}": v for k, v in skipped.items()}
    if n == 0:
        return {"total_trades": 0, "equity_curve": equity, "monthly_stats": [], **base}

    pnls = [t["pnl_net"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    total_pnl = sum(pnls)
    starting = float(config.get("starting_capital", 10000))

    # Drawdown
    caps = [e["capital"] for e in equity]
    peak = caps[0] if caps else starting
    max_dd = 0.0
    max_dd_pct = 0.0
    for c in caps:
        peak = max(peak, c)
        dd = peak - c
        dd_pct = (dd / peak * 100) if peak > 0 else 0
        max_dd = max(max_dd, dd)
        max_dd_pct = max(max_dd_pct, dd_pct)

    # Sharpe / Sortino (annualized by observed trades-per-year cadence)
    rets = [p / starting for p in pnls]
    mean_r = sum(rets) / n
    var_r = sum((r - mean_r) ** 2 for r in rets) / n if n > 1 else 0.0
    std_r = math.sqrt(var_r)
    try:
        span_years = max((_parse_date(equity[-1]["date"]) - _parse_date(equity[0]["date"])).days / 365.25, 0.01)
        tpy = n / span_years
    except Exception:
        tpy = 252.0
    sharpe = (mean_r / std_r) * math.sqrt(tpy) if std_r > 0 else 0.0
    neg = [r for r in rets if r < 0]
    downside_std = math.sqrt(sum(r * r for r in neg) / n) if neg else 0.0
    sortino = (mean_r / downside_std) * math.sqrt(tpy) if downside_std > 0 else 0.0

    gross_wins = sum(wins)
    gross_losses = abs(sum(losses))
    profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else (999.0 if gross_wins > 0 else 0.0)

    reasons = [t["exit_reason"] for t in trades]
    def _pct(tag): return round(sum(1 for r in reasons if r == tag) / n * 100, 1)

    contracts = int(config.get("contracts", 1) or 1)
    total_premium = sum(t["net_credit"] * 100 * contracts for t in trades if t["net_credit"] > 0)

    avg_dte = sum(t["dte_at_entry"] for t in trades) / n
    avg_days = sum(t["days_in_trade"] for t in trades) / n
    avg_iv = sum(t["greeks_at_entry"]["atm_iv"] for t in trades) / n

    short_deltas = [l["delta"] for t in trades for l in t["legs"] if l["action"] == "SELL"]
    avg_entry_delta = (sum(short_deltas) / len(short_deltas)) if short_deltas else 0.0

    # Monthly buckets
    monthly = {}
    for t in trades:
        d = _parse_date(t["exit_date"])
        key = (d.year, d.month)
        m = monthly.setdefault(key, {"year": d.year, "month": d.month, "pnl": 0.0, "trades": 0,
                                     "wins": 0, "best": -float("inf"), "worst": float("inf")})
        m["pnl"] += t["pnl_net"]
        m["trades"] += 1
        if t["pnl_net"] > 0:
            m["wins"] += 1
        m["best"] = max(m["best"], t["pnl_net"])
        m["worst"] = min(m["worst"], t["pnl_net"])
    monthly_stats = []
    for k in sorted(monthly.keys()):
        m = monthly[k]
        mt = m["trades"]
        monthly_stats.append({
            "year": m["year"], "month": m["month"],
            "pnl": round(m["pnl"], 2), "trades": mt,
            "win_rate": round(m["wins"] / mt * 100, 1) if mt else 0.0,
            "avg_pnl": round(m["pnl"] / mt, 2) if mt else 0.0,
            "best": round(m["best"], 2), "worst": round(m["worst"], 2),
        })

    return {
        "total_pnl": round(total_pnl, 2),
        "total_trades": n,
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "win_rate": round(len(wins) / n * 100, 2),
        "avg_pnl_per_trade": round(total_pnl / n, 2),
        "avg_winner": round(sum(wins) / len(wins), 2) if wins else 0.0,
        "avg_loser": round(sum(losses) / len(losses), 2) if losses else 0.0,
        "best_trade": round(max(pnls), 2),
        "worst_trade": round(min(pnls), 2),
        "max_drawdown": round(max_dd, 2),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "sharpe_ratio": round(sharpe, 2),
        "sortino_ratio": round(sortino, 2),
        "profit_factor": round(profit_factor, 2),
        "total_premium_collected": round(total_premium, 2),
        "avg_dte_at_entry": round(avg_dte, 1),
        "avg_days_in_trade": round(avg_days, 1),
        "pct_expired_worthless": _pct("EXPIRED"),
        "pct_closed_profit_target": _pct("PROFIT_TARGET"),
        "pct_closed_stop_loss": _pct("STOP_LOSS"),
        "pct_closed_eod": _pct("EOD"),
        "avg_entry_iv": round(avg_iv, 4),
        "avg_entry_delta": round(avg_entry_delta, 4),
        "equity_curve": equity,
        "monthly_stats": monthly_stats,
        **base,
    }


# ── Main engine ──────────────────────────────────────────────────────────────
def run_options_backtest_stream(config: dict):
    """Generator yielding SSE-shaped events as trades complete.

    Events:
      {"type": "start", "total": N}
      {"type": "progress", "done": i, "total": N, "date": "YYYY-MM-DD", "skipped": "reason"|null}
      {"type": "trade", "trade": {...}}
      {"type": "complete", "metrics": {...}, "trade_log": [...]}
      {"type": "error", "message": "..."}
    """
    symbol = config["symbol"]
    start = config["start_date"]
    end = config["end_date"]
    entry_time = config.get("entry_time", "09:45")
    entry_days = set(config.get("entry_days") or ["Mon", "Tue", "Wed", "Thu", "Fri"])
    frequency = (config.get("entry_frequency") or "DAILY").upper()

    all_dates = _cached_dates(symbol)
    window = [d for d in all_dates if start <= d <= end]
    entry_wds = {_WEEKDAY_MAP[d] for d in entry_days if d in _WEEKDAY_MAP}
    dates = [d for d in window if _parse_date(d).weekday() in entry_wds]

    if frequency == "WEEKLY":
        seen = set()
        filt = []
        for d in dates:
            iso = _parse_date(d).isocalendar()
            key = (iso[0], iso[1])
            if key not in seen:
                seen.add(key)
                filt.append(d)
        dates = filt

    total = len(dates)
    available = len(all_dates)
    in_window = len(window)
    print(f"[backtest] {symbol}: {available} available dates in bucket, "
          f"period {start} -> {end}, {in_window} in-window, "
          f"{total} entry dates after weekday/frequency filter")
    yield {"type": "start", "total": total, "symbol": symbol,
           "strategy": config["strategy_type"], "start": start, "end": end,
           "available_dates": available, "entry_dates": total,
           "message": f"Found {total} entry dates for {symbol}"}

    # Kick off parallel downloads of every trading date in the window. Each
    # trade's entry + prewarm then block only on dates not yet finished.
    window_dates = [d for d in all_dates if start <= d <= end]
    _prewarm_all_window_dates(symbol, window_dates)

    # On cold cache the fire-and-forget prewarm hasn't finished for the first
    # trade date yet, so get_chain_for_date would return empty and the engine
    # would mark the day as no_chain. Wait up to 30s for the first entry
    # date's parquet to land on disk, surfacing a "downloading" progress
    # event so the UI doesn't look frozen.
    first_date = dates[0] if dates else None
    if first_date:
        first_cf = options_data._cache_path(symbol.upper(), first_date)
        if not first_cf.exists():
            yield {"type": "progress", "done": 0, "total": total,
                   "date": first_date, "skipped": None,
                   "message": "Downloading data for first trade date..."}
            for _ in range(60):  # 60 * 0.5s = 30s max
                if first_cf.exists():
                    break
                time.sleep(0.5)

    trades = []
    capital = float(config.get("starting_capital", 10000))
    equity = [{"date": start, "capital": capital}]
    skipped = {"no_chain": 0, "no_expiry": 0, "no_strike": 0,
               "iv_filter": 0, "premium_filter": 0, "spread_filter": 0, "overlap": 0}
    last_exit_date: Optional[str] = None

    target_dte = int(config.get("target_dte", 7))
    tol = int(config.get("dte_tolerance", 2))

    for i, entry_date in enumerate(dates):
        # Overlap: skip if a prior trade is still open or exited today
        if last_exit_date is not None and entry_date <= last_exit_date:
            skipped["overlap"] += 1
            yield {"type": "progress", "done": i + 1, "total": total,
                   "date": entry_date, "skipped": "overlap"}
            continue

        try:
            chain = options_data.get_chain_for_date(symbol, entry_date, entry_time)
        except Exception as e:
            log.debug("entry chain fetch failed %s %s: %s", symbol, entry_date, e)
            skipped["no_chain"] += 1
            yield {"type": "progress", "done": i + 1, "total": total,
                   "date": entry_date, "skipped": "no_chain",
                   "message": "Downloading data..."}
            continue

        if chain.empty:
            skipped["no_chain"] += 1
            yield {"type": "progress", "done": i + 1, "total": total,
                   "date": entry_date, "skipped": "no_chain",
                   "message": "Downloading data..."}
            continue

        # Find target expiry
        min_dte = max(0, target_dte - tol)
        max_dte = target_dte + tol
        expiries = options_data.get_available_expiries(chain, min_dte=min_dte, max_dte=max_dte)
        if not expiries:
            skipped["no_expiry"] += 1
            yield {"type": "progress", "done": i + 1, "total": total,
                   "date": entry_date, "skipped": "no_expiry"}
            continue
        ref_date = _parse_date(entry_date)
        expiry = min(expiries, key=lambda e: abs((_parse_date(e) - ref_date).days - target_dte))
        expiry = str(expiry)[:10]
        dte_at_entry = (_parse_date(expiry) - ref_date).days

        # Build legs
        try:
            legs = _select_legs(config, chain, expiry)
        except Exception as e:
            log.warning("leg selection error %s %s: %s", symbol, entry_date, e)
            legs = None
        if not legs:
            skipped["no_strike"] += 1
            yield {"type": "progress", "done": i + 1, "total": total,
                   "date": entry_date, "skipped": "no_strike"}
            continue

        # Filters: IV, premium, bid-ask spread
        spot = _underlying_at(chain)
        atm_iv = compute_atm_iv(chain, spot, expiry)
        min_iv = config.get("min_entry_iv")
        max_iv = config.get("max_entry_iv")
        if atm_iv is not None:
            if min_iv is not None and atm_iv < float(min_iv):
                skipped["iv_filter"] += 1
                yield {"type": "progress", "done": i + 1, "total": total,
                       "date": entry_date, "skipped": "iv_filter"}
                continue
            if max_iv is not None and atm_iv > float(max_iv):
                skipped["iv_filter"] += 1
                yield {"type": "progress", "done": i + 1, "total": total,
                       "date": entry_date, "skipped": "iv_filter"}
                continue

        net_credit = sum(l["fill_price"] if l["action"] == "SELL" else -l["fill_price"] for l in legs)

        min_prem = config.get("min_entry_premium")
        if min_prem is not None and net_credit < float(min_prem):
            skipped["premium_filter"] += 1
            yield {"type": "progress", "done": i + 1, "total": total,
                   "date": entry_date, "skipped": "premium_filter"}
            continue

        max_spread = config.get("max_bid_ask_spread_pct")
        if max_spread is not None:
            bad = False
            for l in legs:
                mid = l["entry_mid"]
                if mid <= 0:
                    bad = True
                    break
                sp = (l["entry_ask"] - l["entry_bid"]) / mid
                if sp > float(max_spread):
                    bad = True
                    break
            if bad:
                skipped["spread_filter"] += 1
                yield {"type": "progress", "done": i + 1, "total": total,
                       "date": entry_date, "skipped": "spread_filter"}
                continue

        contracts = int(config.get("contracts", 1) or 1)
        max_profit = max(net_credit, 0.0) * 100 * contracts
        max_loss = _compute_max_loss(legs, net_credit, contracts)

        pos = {
            "entry_date": entry_date,
            "entry_time": entry_time,
            "expiry": expiry,
            "dte_at_entry": dte_at_entry,
            "legs": legs,
            "underlying_at_entry": spot,
            "net_credit": net_credit,
            "max_profit": max_profit,
            "max_loss": max_loss,
            "atm_iv_entry": atm_iv,
        }

        # Prewarm disk cache for all exit dates in parallel (no-op if warm).
        _prewarm_exit_dates(symbol, entry_date, expiry)
        exit_result = _run_exit_phase(pos, config)
        trade = _finalize_trade(pos, exit_result, config)
        trades.append(trade)
        capital += trade["pnl_net"]
        equity.append({"date": trade["exit_date"], "capital": round(capital, 2)})
        last_exit_date = trade["exit_date"]

        yield {"type": "progress", "done": i + 1, "total": total,
               "date": entry_date, "skipped": None}
        yield {"type": "trade", "trade": trade}

    metrics = _compute_metrics(trades, config, skipped, equity)
    yield {"type": "complete", "metrics": metrics, "trade_log": trades}


def run_options_backtest(config: dict) -> dict:
    """Non-streaming wrapper. Returns {"metrics": {...}, "trade_log": [...]}."""
    trade_log: list = []
    metrics: dict = {}
    for event in run_options_backtest_stream(config):
        if event["type"] == "trade":
            trade_log.append(event["trade"])
        elif event["type"] == "complete":
            metrics = event["metrics"]
        elif event["type"] == "error":
            raise RuntimeError(event.get("message", "options backtest error"))
    return {"metrics": metrics, "trade_log": trade_log}
