"""Smoke test for api/options_backtest.py -- SHORT_PUT_SPREAD on SPY.

Defaults to a 2-month window for quick iteration. Override via env vars to
run the full spec period:

  BT_START=2023-01-01 BT_END=2023-12-31 python test_options_backtest.py
"""

import os
import sys
import time
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.options_backtest import run_options_backtest


CONFIG = {
    "symbol": "SPY",
    "strategy_type": "SHORT_PUT_SPREAD",

    # DTE & Expiry
    "target_dte": 7,
    "dte_tolerance": 2,

    # Strikes
    "short_strike_method": "DELTA",
    "short_strike_value": -0.30,
    "wing_width_method": "POINTS",
    "wing_width_value": 5.0,

    # Entry
    "entry_time": "09:45",
    "entry_days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
    "entry_frequency": "DAILY",

    # Exits
    "profit_target_pct": 50,
    "stop_loss_pct": 200,
    "exit_time": "15:45",
    "check_exit_times": ["10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "15:45"],

    # Sizing
    "starting_capital": 10000,
    "contracts": 1,
    "commission_per_contract": 0.65,
    "slippage_pct": 0.0,

    # Period
    "start_date": os.environ.get("BT_START", "2023-01-01"),
    "end_date": os.environ.get("BT_END", "2023-02-28"),
}


def fmt_leg(leg):
    sign = "-" if leg["action"] == "SELL" else "+"
    return (f"    {sign} {leg['action']:4s} {leg['right']} {leg['strike']:>7.2f} "
            f"@ ${leg['fill_price']:.2f}  delta={leg['delta']:+.3f}  IV={leg['iv']:.2%}")


def print_trade(i, t):
    print(f"\n--- Trade #{i} --- {t['exit_reason']}")
    print(f"  Entry: {t['entry_date']} {t['entry_time']}   Exit: {t['exit_date']} {t['exit_time']}  ({t['days_in_trade']}d)")
    print(f"  Expiry: {t['expiration']} (DTE at entry: {t['dte_at_entry']})")
    print(f"  Underlying: ${t['underlying_at_entry']:.2f} -> ${t['underlying_at_exit']:.2f}")
    print(f"  Legs:")
    for l in t["legs"]:
        print(fmt_leg(l))
    print(f"  Net credit:    ${t['net_credit']:.2f}/sh  (max profit ${t['max_profit']:.2f})")
    print(f"  Cost to close: ${t['cost_to_close']:.2f}/sh")
    print(f"  Gross PnL:     ${t['pnl_gross']:+.2f}")
    print(f"  Commission:    ${t['commission']:.2f}")
    print(f"  Net PnL:       ${t['pnl_net']:+.2f}   ({t['pnl_pct_of_max']:+.1f}% of max)")
    g = t["greeks_at_entry"]
    print(f"  Greeks@entry:  d {g['net_delta']:+.3f}  theta {g['net_theta']:+.3f}  gamma {g['net_gamma']:+.4f}  vega {g['net_vega']:+.3f}  ATM IV {g['atm_iv']:.2%}")


def print_metrics(m):
    print(f"\n=== BACKTEST RESULTS ===")
    print(f"  Period:        {CONFIG['start_date']} -> {CONFIG['end_date']}")
    print(f"  Symbol:        {CONFIG['symbol']}  Strategy: {CONFIG['strategy_type']}")
    print(f"\n  Total PnL:     ${m['total_pnl']:+,.2f}")
    print(f"  Total trades:  {m['total_trades']}   Wins: {m['winning_trades']}   Losses: {m['losing_trades']}")
    print(f"  Win rate:      {m['win_rate']:.1f}%")
    print(f"  Avg/trade:     ${m['avg_pnl_per_trade']:+.2f}")
    print(f"  Avg winner:    ${m['avg_winner']:+.2f}   Avg loser: ${m['avg_loser']:+.2f}")
    print(f"  Best:          ${m['best_trade']:+.2f}   Worst: ${m['worst_trade']:+.2f}")
    print(f"\n  Max drawdown:  ${m['max_drawdown']:,.2f}  ({m['max_drawdown_pct']:.1f}%)")
    print(f"  Sharpe:        {m['sharpe_ratio']:.2f}   Sortino: {m['sortino_ratio']:.2f}")
    print(f"  Profit factor: {m['profit_factor']:.2f}")
    print(f"\n  Premium collected: ${m['total_premium_collected']:,.2f}")
    print(f"  Avg DTE@entry:     {m['avg_dte_at_entry']:.1f}   Avg days in trade: {m['avg_days_in_trade']:.1f}")
    print(f"  Avg entry IV:      {m['avg_entry_iv']:.2%}  Avg short delta: {m['avg_entry_delta']:+.3f}")
    print(f"\n  Exit reasons:")
    print(f"    Profit target: {m['pct_closed_profit_target']:.1f}%")
    print(f"    Stop loss:     {m['pct_closed_stop_loss']:.1f}%")
    print(f"    Expired:       {m['pct_expired_worthless']:.1f}%")
    print(f"    EOD:           {m['pct_closed_eod']:.1f}%")
    print(f"\n  Skipped days:")
    for k in sorted(k for k in m if k.startswith("skipped_")):
        print(f"    {k[8:]:<18s} {m[k]}")
    if m.get("monthly_stats"):
        print(f"\n  Monthly breakdown:")
        print(f"    {'Month':<10} {'Trades':>7} {'PnL':>10} {'Avg':>9} {'WinRate':>8}")
        for ms in m["monthly_stats"]:
            print(f"    {ms['year']}-{ms['month']:02d}    {ms['trades']:>7} {ms['pnl']:>+10.2f} {ms['avg_pnl']:>+9.2f} {ms['win_rate']:>7.1f}%")


def main():
    print(f"Running options backtest: {CONFIG['symbol']} {CONFIG['strategy_type']}")
    print(f"  {CONFIG['start_date']} -> {CONFIG['end_date']}, {CONFIG['target_dte']}DTE, delta={CONFIG['short_strike_value']}, ${CONFIG['wing_width_value']} wide")
    print(f"  Entry {CONFIG['entry_time']}, PT {CONFIG['profit_target_pct']}%, SL {CONFIG['stop_loss_pct']}%")
    print(f"  (Override period with BT_START / BT_END env vars)")
    t0 = time.time()
    result = run_options_backtest(CONFIG)
    elapsed = time.time() - t0
    trades = result["trade_log"]
    metrics = result["metrics"]
    print(f"\nCompleted in {elapsed:.1f}s  ({len(trades)} trades)")

    for i, tr in enumerate(trades[:5], 1):
        print_trade(i, tr)

    print_metrics(metrics)

    # Assertions
    assert metrics["total_trades"] > 0, "no trades produced"
    assert metrics["win_rate"] > 0, f"win_rate is {metrics['win_rate']}"
    print("\nAssertions passed.")


def test_cache_performance():
    """Cold-vs-warm benchmark: full year 2023 SPY SHORT_PUT_SPREAD."""
    from api.options_data import clear_cache, get_cache_stats

    config = {
        "symbol": "SPY",
        "strategy_type": "SHORT_PUT_SPREAD",
        "target_dte": 7,
        "dte_tolerance": 2,
        "short_strike_method": "DELTA",
        "short_strike_value": -0.30,
        "wing_width_method": "POINTS",
        "wing_width_value": 5.0,
        "entry_time": "09:45",
        "entry_days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
        "entry_frequency": "DAILY",
        "profit_target_pct": 50,
        "stop_loss_pct": 200,
        "exit_time": "15:45",
        "check_exit_times": ["10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "15:45"],
        "starting_capital": 10000,
        "contracts": 1,
        "commission_per_contract": 0.65,
        "slippage_pct": 0.0,
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
    }

    # Cold start: wipe SPY cache first
    print("\n=== Clearing SPY cache for cold run ===")
    cleared = clear_cache("SPY")
    print(f"Cleared {cleared['deleted_files']} files ({cleared['freed_mb']} MB)")

    print("\n=== COLD CACHE RUN (full year 2023) ===")
    t0 = time.time()
    results = run_options_backtest(config)
    cold_time = time.time() - t0
    print(f"Cold run: {cold_time:.1f}s | {results['metrics']['total_trades']} trades")

    print("\n=== WARM CACHE RUN (same year, all local) ===")
    t0 = time.time()
    results2 = run_options_backtest(config)
    warm_time = time.time() - t0
    speedup = cold_time / warm_time if warm_time > 0 else float("inf")
    print(f"Warm run: {warm_time:.1f}s | speedup: {speedup:.1f}x")

    assert results["metrics"]["total_trades"] == results2["metrics"]["total_trades"], \
        "trade count differs between cold and warm"
    assert abs(results["metrics"]["total_pnl"] - results2["metrics"]["total_pnl"]) < 0.01, \
        f"PnL differs: cold={results['metrics']['total_pnl']} warm={results2['metrics']['total_pnl']}"
    print("Results identical between cold and warm runs.")

    stats = get_cache_stats()
    print(f"\nCache: {stats['total_files']} files, {stats['total_size_mb']:.1f} MB total")
    for s in stats["symbols"]:
        print(f"  {s['symbol']}: {s['files']} files, {s['size_mb']} MB")

    print_metrics(results["metrics"])


if __name__ == "__main__":
    import sys
    if "--bench" in sys.argv or os.environ.get("BT_BENCH"):
        test_cache_performance()
    else:
        main()
