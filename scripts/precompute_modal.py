"""QuantX -- Modal Precompute Engine.

Runs options backtests in parallel on Modal's serverless infrastructure.
Each task (symbol × strategy × DTE × period) runs in its own container,
reading data from R2 and saving results back to R2.

302 minutes locally → ~15-20 minutes on Modal (parallel containers)
Cost: ~$0.50-2.00 per full run

Usage (from repo root):
    pip install modal
    modal run scripts/precompute_modal.py                    # spxw_0dte phase
    modal run scripts/precompute_modal.py --phase spy_7dte
    modal run scripts/precompute_modal.py --phase all
    modal run scripts/precompute_modal.py --dry-run          # list tasks only
    modal run scripts/precompute_modal.py --no-skip          # recompute all
"""

from __future__ import annotations

import modal
import os
import sys

# ── Modal app definition ──────────────────────────────────────────────────────

app = modal.App("quantx-precompute")

# Container image — installs all deps the backtest engine needs.
# Uses Python 3.11 to match Railway (longport needs 3.11).
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install([
        "fastapi==0.111.0",
        "pandas>=2.0",
        "pyarrow>=14.0",
        "boto3>=1.34",
        "numpy>=1.26",
        "scipy>=1.12",
        "cryptography>=42.0",
    ])

    .env({
        "OPTIONS_CACHE_DIR": "/tmp/options_cache",
        "OPTIONS_USE_SLIM_DATA": "true",
    })
    .add_local_dir("./api", remote_path="/root/api")
)


# ── R2 credentials (read from environment) ────────────────────────────────────
# Set these once with:  modal secret create quantx-r2
#   R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY       (options data bucket)
#   R2_RESULTS_ACCESS_KEY, R2_RESULTS_SECRET_KEY    (results bucket)
#   R2_BUCKET, R2_BACKTEST_BUCKET
r2_secret = modal.Secret.from_name("quantx-r2")

# ── Phase configs (mirrors precompute_spxw.py) ────────────────────────────────
PHASE_CONFIGS = {
    "spxw_0dte": {
        "symbols": ["SPXW"],
        "strategies": [
            "SHORT_PUT_SPREAD", "SHORT_CALL_SPREAD", "IRON_CONDOR",
            "SHORT_STRANGLE", "LONG_CALL", "LONG_PUT",
        ],
        "dtes": [0],
        "periods": {"1Y": 1, "2Y": 2, "3Y": 3},
        "entry_params_override": {},
    },
    "spy_7dte": {
        "symbols": ["SPY"],
        "strategies": [
            "SHORT_PUT_SPREAD", "SHORT_CALL_SPREAD", "IRON_CONDOR",
            "SHORT_STRANGLE", "LONG_CALL", "LONG_PUT",
        ],
        "dtes": [7],
        "periods": {"1Y": 1, "2Y": 2, "3Y": 3},
        "entry_params_override": {
            "dte_tolerance": 2,
            "check_exit_times": ["10:00", "12:00", "14:00", "15:45"],
        },
    },
    "spy_all": {
        "symbols": ["SPY"],
        "strategies": [
            "SHORT_PUT_SPREAD", "SHORT_CALL_SPREAD",
            "IRON_CONDOR", "SHORT_STRANGLE",
        ],
        "dtes": [0, 1, 7, 14, 21, 30],
        "periods": {"1Y": 1, "2Y": 2, "3Y": 3},
        "entry_params_override": {},
    },
}

# Default backtest params (mirrors precompute_spxw.py)
DEFAULT_PARAMS = {
    "short_strike_method": "DELTA",
    "short_strike_value": -0.30,
    "short_call_strike_method": "DELTA",
    "short_call_strike_value": 0.30,
    "wing_width_method": "POINTS",
    "wing_width_value": 5.0,
    "call_wing_width_value": 5.0,
    "profit_target_pct": 50,
    "stop_loss_pct": 200,
}

ENTRY_PARAMS = {
    "entry_time": "09:45",
    "entry_days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
    "entry_frequency": "DAILY",
    "dte_tolerance": 0,
    "contracts": 1,
    "commission_per_contract": 0.65,
    "slippage_pct": 0.0,
    "starting_capital": 10000,
    "check_exit_times": [
        "09:45", "10:00", "10:30", "11:00", "11:30",
        "12:00", "12:30", "13:00", "13:30", "14:00",
        "14:30", "15:00", "15:30", "15:45",
    ],
    "exit_time": "15:45",
}

R2_RESULTS_ENDPOINT = "https://7f835882a6c11ee760fe4e96eb8cbef2.r2.cloudflarestorage.com"
R2_RESULTS_BUCKET   = "quantx-results"


# ── Helper: build config for one task ─────────────────────────────────────────

def _build_config(symbol: str, strategy: str, dte: int,
                  period_years: int, entry_override: dict) -> dict:
    from datetime import date, timedelta
    end_date   = date.today()
    start_date = end_date - timedelta(days=365 * period_years)
    params = {**ENTRY_PARAMS, **entry_override}
    return {
        "symbol":                    symbol,
        "strategy_type":             strategy,
        "target_dte":                dte,
        "dte_tolerance":             params.get("dte_tolerance", 0),
        # Strike selection (exact keys options_backtest.py expects)
        "short_strike_method":       DEFAULT_PARAMS["short_strike_method"],
        "short_strike_value":        DEFAULT_PARAMS["short_strike_value"],
        "short_call_strike_method":  DEFAULT_PARAMS["short_call_strike_method"],
        "short_call_strike_value":   DEFAULT_PARAMS["short_call_strike_value"],
        "wing_width_method":         DEFAULT_PARAMS["wing_width_method"],
        "wing_width_value":          DEFAULT_PARAMS["wing_width_value"],
        "call_wing_width_value":     DEFAULT_PARAMS["call_wing_width_value"],
        # P&L management
        "profit_target_pct":         DEFAULT_PARAMS["profit_target_pct"] / 100,
        "stop_loss_mult":            DEFAULT_PARAMS["stop_loss_pct"] / 100,
        # Timing
        "entry_time_et":             params["entry_time"],
        "exit_time_et":              params["exit_time"],
        "entry_days":                params["entry_days"],
        "check_exit_times":          params["check_exit_times"],
        # Execution
        "contracts":                 params["contracts"],
        "commission_per_contract":   params["commission_per_contract"],
        "slippage_pct":              params["slippage_pct"],
        "starting_capital":          params["starting_capital"],
        "start_date":                start_date.isoformat(),
        "end_date":                  end_date.isoformat(),
        "dry_run":                   False,
    }


def _result_key(symbol, strategy, dte, period, params_hash="default") -> str:
    return f"results/{symbol}/{strategy}/{dte}DTE/{period}/{params_hash}.json"


def _already_computed(symbol, strategy, dte, period) -> bool:
    """Check if result already exists in R2."""
    import boto3
    key = _result_key(symbol, strategy, dte, period)
    s3 = boto3.client(
        "s3",
        endpoint_url=R2_RESULTS_ENDPOINT,
        aws_access_key_id=os.environ.get("R2_RESULTS_ACCESS_KEY",
                                          os.environ.get("R2_ACCESS_KEY", "")),
        aws_secret_access_key=os.environ.get("R2_RESULTS_SECRET_KEY",
                                              os.environ.get("R2_SECRET_KEY", "")),
        region_name="auto",
    )
    try:
        s3.head_object(Bucket=R2_RESULTS_BUCKET, Key=key)
        return True
    except Exception:
        return False


# ── Modal function: one container per task ────────────────────────────────────

@app.function(
    image=image,
    secrets=[r2_secret],
    timeout=3600,          # 1 hour max per task (3Y SPXW takes ~2h locally, ~20min on Modal)
    memory=4096,           # 4GB RAM per container
    cpu=2,
    retries=1,
)
def run_task(task: dict) -> dict:
    """Run a single backtest task in a Modal container.

    Each container:
    - Downloads needed parquet files from R2 into /tmp/options_cache/
    - Runs the full backtest
    - Saves result JSON to R2
    - Returns summary dict
    """
    import json
    import time
    import boto3
    from datetime import datetime
    from pathlib import Path

    symbol   = task["symbol"]
    strategy = task["strategy"]
    dte      = task["dte"]
    period   = task["period"]
    entry_override = task.get("entry_override", {})

    print(f"[{symbol} {strategy} {dte}DTE {period}] Starting...")

    # Add project root to path so api.* imports work
    sys.path.insert(0, "/root")

    t0 = time.time()
    try:
        from api.options_backtest import run_options_backtest
        config = _build_config(symbol, strategy, dte, task["period_years"], entry_override)
        result = run_options_backtest(config)
        elapsed = time.time() - t0

        m = result["metrics"]
        print(
            f"[{symbol} {strategy} {dte}DTE {period}] "
            f"OK | {m.get('total_trades',0)} trades | "
            f"Sharpe={m.get('sharpe_ratio',0):.2f} | "
            f"WR={m.get('win_rate',0):.1f}% | "
            f"PnL=${m.get('total_pnl',0):.0f} | {elapsed:.0f}s"
        )

        # Save result to R2
        s3 = boto3.client(
            "s3",
            endpoint_url=R2_RESULTS_ENDPOINT,
            aws_access_key_id=os.environ.get("R2_RESULTS_ACCESS_KEY",
                                              os.environ.get("R2_ACCESS_KEY", "")),
            aws_secret_access_key=os.environ.get("R2_RESULTS_SECRET_KEY",
                                                  os.environ.get("R2_SECRET_KEY", "")),
            region_name="auto",
        )

        payload = {
            "symbol": symbol, "strategy": strategy,
            "dte": dte, "period": period, "params_hash": "default",
            "computed_at": datetime.utcnow().isoformat(),
            "metrics": result["metrics"],
            "trade_log": result["trade_log"],
        }
        key = _result_key(symbol, strategy, dte, period)
        s3.put_object(
            Bucket=R2_RESULTS_BUCKET,
            Key=key,
            Body=json.dumps(payload, default=str).encode(),
            ContentType="application/json",
        )
        print(f"[{symbol} {strategy} {dte}DTE {period}] Saved to R2: {key}")

        return {
            "status": "ok", "task": task, "elapsed": elapsed,
            "sharpe": m.get("sharpe_ratio", 0),
            "trades": m.get("total_trades", 0),
            "pnl":    m.get("total_pnl", 0),
            "win_rate": m.get("win_rate", 0),
        }

    except Exception as e:
        elapsed = time.time() - t0
        print(f"[{symbol} {strategy} {dte}DTE {period}] ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "task": task, "error": str(e), "elapsed": elapsed}


# ── Modal function: live student backtest slice ───────────────────────────────
#
# Called by /api/options/backtest-fast on Railway for periods > 60 days.
# Railway splits the full date range into monthly slices and dispatches
# them all in parallel via run_backtest_slice.map(slices).
# Each slice returns its raw trade list (not metrics) so the orchestrator
# can merge, sort, and recompute metrics once over the full timeline.
#
# Overlap note: the sequential `last_exit_date` guard inside
# run_options_backtest_stream cannot span slice boundaries.  For typical
# holding periods (0–7 DTE) the boundary error is at most one trade per
# month — negligible on a 1-year backtest.  The orchestrator sorts by
# entry_date and re-filters overlaps after merge to correct this.

@app.function(
    image=image,
    secrets=[r2_secret],
    timeout=300,    # 5 min per month slice is very generous
    memory=2048,    # 2 GB: one month of parquets fits easily
    cpu=2,
    retries=1,
)
def run_backtest_slice(slice_config: dict) -> dict:
    """Run one monthly slice of an options backtest.

    Args:
        slice_config: full backtest config dict with `start_date` and
                      `end_date` already narrowed to this month's range,
                      plus `_slice_index` (int) for ordering.

    Returns:
        {
            "status": "ok" | "error",
            "slice_index": int,
            "trades": [...],   # raw trade dicts from this slice
            "error": str,      # only on error
        }
    """
    import time
    sys.path.insert(0, "/root")

    slice_index = slice_config.get("_slice_index", 0)
    symbol   = slice_config.get("symbol", "?")
    start    = slice_config.get("start_date", "?")
    end      = slice_config.get("end_date", "?")
    print(f"[slice {slice_index}] {symbol} {start} → {end}")

    t0 = time.time()
    try:
        from api.options_backtest import run_options_backtest

        # Strip internal keys before passing to the engine
        config = {k: v for k, v in slice_config.items() if not k.startswith("_")}
        result = run_options_backtest(config)
        trades = result.get("trade_log", [])
        elapsed = time.time() - t0
        print(f"[slice {slice_index}] OK — {len(trades)} trades in {elapsed:.1f}s")
        return {"status": "ok", "slice_index": slice_index, "trades": trades}

    except Exception as e:
        import traceback
        elapsed = time.time() - t0
        print(f"[slice {slice_index}] ERROR after {elapsed:.1f}s: {e}")
        traceback.print_exc()
        return {"status": "error", "slice_index": slice_index, "trades": [], "error": str(e)}


# ── Local entrypoint ──────────────────────────────────────────────────────────

@app.local_entrypoint()
def main(
    phase: str = "spxw_0dte",
    dry_run: bool = False,
    no_skip: bool = False,
    sharpe_threshold: float = 1.5,
):
    """
    Run options precompute on Modal.

    Examples:
        modal run scripts/precompute_modal.py
        modal run scripts/precompute_modal.py --phase spy_7dte
        modal run scripts/precompute_modal.py --phase all
        modal run scripts/precompute_modal.py --dry-run
        modal run scripts/precompute_modal.py --no-skip
    """
    import json

    # Build phase list
    if phase == "all":
        phases = list(PHASE_CONFIGS.keys())
    elif phase in PHASE_CONFIGS:
        phases = [phase]
    else:
        print(f"Unknown phase: {phase}. Options: {list(PHASE_CONFIGS.keys()) + ['all']}")
        return

    # Build full task list
    all_tasks = []
    for ph in phases:
        cfg = PHASE_CONFIGS[ph]
        entry_override = cfg.get("entry_params_override", {})
        for symbol in cfg["symbols"]:
            for strategy in cfg["strategies"]:
                for dte in cfg["dtes"]:
                    for period, years in cfg["periods"].items():
                        all_tasks.append({
                            "symbol":        symbol,
                            "strategy":      strategy,
                            "dte":           dte,
                            "period":        period,
                            "period_years":  years,
                            "entry_override": entry_override,
                        })

    # Filter already-computed (unless --no-skip)
    if not no_skip:
        tasks = []
        for t in all_tasks:
            if _already_computed(t["symbol"], t["strategy"], t["dte"], t["period"]):
                print(f"SKIP (exists): {t['symbol']} {t['strategy']} {t['dte']}DTE {t['period']}")
            else:
                tasks.append(t)
    else:
        tasks = all_tasks

    print("=" * 60)
    print(f"QuantX Modal Precompute — phase: {phase}")
    print(f"  Total tasks:   {len(all_tasks)}")
    print(f"  To run:        {len(tasks)}")
    print(f"  Skipped:       {len(all_tasks) - len(tasks)}")
    print("=" * 60)

    if dry_run:
        for t in tasks:
            print(f"  {t['symbol']} {t['strategy']} {t['dte']}DTE {t['period']}")
        print(f"\nTotal: {len(tasks)} tasks (dry run — nothing submitted)")
        return

    if not tasks:
        print("Nothing to compute — all results up to date.")
        return

    # Submit all tasks in parallel to Modal
    print(f"\nSubmitting {len(tasks)} tasks to Modal (all parallel)...")
    results = list(run_task.map(tasks, return_exceptions=True))

    # Summary
    ok     = [r for r in results if isinstance(r, dict) and r.get("status") == "ok"]
    errors = [r for r in results if isinstance(r, dict) and r.get("status") == "error"]
    exceptions = [r for r in results if isinstance(r, Exception)]

    print("\n" + "=" * 60)
    print(f"COMPLETE: {len(ok)} ok, {len(errors)} errors, {len(exceptions)} exceptions")

    # High sharpe discoveries
    high_sharpe = [r for r in ok if r.get("sharpe", 0) >= sharpe_threshold and r.get("trades", 0) >= 10]
    if high_sharpe:
        print(f"\nHIGH SHARPE STRATEGIES (Sharpe >= {sharpe_threshold}):")
        print(f"{'Symbol':<7} {'Strategy':<22} {'DTE':<4} {'Per':<4} {'Sharpe':>7} {'WR%':>6} {'P&L':>10}")
        print("-" * 65)
        for r in sorted(high_sharpe, key=lambda x: x["sharpe"], reverse=True):
            t = r["task"]
            print(f"{t['symbol']:<7} {t['strategy']:<22} {t['dte']:<4} "
                  f"{t['period']:<4} {r['sharpe']:>7.2f} "
                  f"{r['win_rate']:>5.1f}% ${r['pnl']:>8,.0f}")
    else:
        print(f"No strategies above Sharpe {sharpe_threshold}")

    if errors:
        print(f"\nERRORS ({len(errors)}):")
        for r in errors:
            t = r["task"]
            print(f"  {t['symbol']} {t['strategy']} {t['dte']}DTE {t['period']}: {r['error']}")

    print("=" * 60)
