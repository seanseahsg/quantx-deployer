"""QuantX Deployer — Student trading bot manager. Runs on VPS or Railway."""

import os
import sys
import ast
import subprocess
import signal
import json
import random
import logging as _logging
import time
from collections import deque
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from collections import defaultdict

import asyncio
from concurrent.futures import ThreadPoolExecutor

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from api.config import (
    CENTRAL_API_URL, HOSTING, VERSION, BOTS_DIR, LOGS_DIR, PYTHON_EXE,
)
from api.database import (
    init_db, get_db, save_student, get_student, save_strategy, get_strategies,
    delete_strategy, toggle_strategy, save_process, update_process_status,
    get_latest_process, log_trade, get_trades, decrypt,
    save_ibkr_config, get_ibkr_config,
)
from api.generate import generate_master_bot, generate_ibkr_bot

_log = _logging.getLogger("quantx-deployer")


# ── Models ──────────────────────────────────────────────────────────────────

class RegisterReq(BaseModel):
    email: str
    name: str = ""
    app_key: str
    app_secret: str
    access_token: str
    central_api_url: str = ""


class StrategyReq(BaseModel):
    email: str
    strategy_id: str
    strategy_name: str = ""
    symbol: str = ""
    arena: str = "US"
    timeframe: str = "1m"
    conditions: dict = {}
    exit_rules: dict = {}
    risk: dict = {}
    is_active: bool = True
    mode: str = "library"
    library_id: str = ""
    custom_script: str = ""
    broker: str = "longport"


class IBKRConfigReq(BaseModel):
    email: str
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 1


class ValidateScriptReq(BaseModel):
    script: str


class DeployReq(BaseModel):
    email: str


class StopReq(BaseModel):
    email: str


class SettingsReq(BaseModel):
    email: str
    central_api_url: str = ""


class BacktestReq(BaseModel):
    symbol: str = "TQQQ.US"
    timeframe: str = "1day"
    strategy: str = "TURTLE"
    params: dict = {}
    initial_capital: float = 10000
    limit: int = 1260
    commission_pct: float = 0.0
    slippage_pct: float = 0.0


class OptimizeReq(BaseModel):
    symbol: str = "TQQQ.US"
    timeframe: str = "1day"
    strategy: str = "TURTLE"
    param_grid: dict = {}
    initial_capital: float = 10000
    limit: int = 1260


class ScriptBacktestReq(BaseModel):
    symbol: str = "TQQQ.US"
    timeframe: str = "1day"
    script: str = ""
    initial_capital: float = 10000
    limit: int = 1260
    params: dict = {}


class SweepScriptReq(BaseModel):
    symbol: str = "TQQQ.US"
    timeframe: str = "1day"
    script: str = ""
    initial_capital: float = 10000
    limit: int = 1260
    combos: list = []
    stop_loss_pct: float = 0.0
    take_profit_pct: float = 0.0


class ScreenNowReq(BaseModel):
    email: str
    strategy_id: str
    bot_type: str
    arena: str = "US"
    custom_tickers: list = []


class DownloadScriptReq(BaseModel):
    email: str
    library_id: str
    strategy_id: str = ""
    symbol: str = "TQQQ.US"
    arena: str = "US"
    timeframe: str = "5m"
    params: dict = {}
    risk: dict = {}


class TradeReq(BaseModel):
    email: str
    student_name: str = ""
    strategy_id: str = ""
    symbol: str = ""
    side: str = ""
    price: float = 0.0
    qty: float = 0.0
    pnl: float = 0.0


# ── State ───────────────────────────────────────────────────────────────────

_running_processes: dict[str, subprocess.Popen] = {}


# ── App ─────────────────────────────────────────────────────────────────────

def _launch_bot(script_path: str, log_path: str) -> subprocess.Popen:
    """Cross-platform bot launcher."""
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    log_fh = open(log_path, "a", encoding="utf-8")
    kwargs = {
        "stdout": log_fh, "stderr": log_fh,
        "cwd": os.path.dirname(script_path),
    }
    if os.name == "nt":
        kwargs["creationflags"] = 0x08000000  # CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen([PYTHON_EXE, script_path], **kwargs)


def _auto_restart_bots():
    """Re-launch bots that were running before a server restart (Railway)."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT email, master_script_path, log_path FROM processes WHERE status = 'running'"
        ).fetchall()
    finally:
        conn.close()
    restarted = 0
    for row in rows:
        script = row["master_script_path"]
        if script and Path(script).exists():
            try:
                proc = _launch_bot(script, row["log_path"])
                _running_processes[row["email"]] = proc
                conn2 = get_db()
                conn2.execute("UPDATE processes SET pid = ? WHERE email = ? AND status = 'running'",
                              (proc.pid, row["email"]))
                conn2.commit()
                conn2.close()
                restarted += 1
                _log.info("[STARTUP] Re-launched bot for %s (PID %s)", row["email"], proc.pid)
            except Exception as e:
                _log.warning("[STARTUP] Failed to re-launch for %s: %s", row["email"], e)
    if restarted:
        _log.info("[STARTUP] Re-launched %d bots after restart", restarted)


def _startup_prewarm():
    """Background prewarm top 10 symbols on startup."""
    try:
        from api.backtest import prewarm_symbol, PREWARM_PRIORITY
        for sym in PREWARM_PRIORITY:
            try:
                r = prewarm_symbol(sym, "1day")
                _log.info("[PREWARM] %s: %s", sym, r.get("status", "?"))
            except Exception as e:
                _log.warning("[PREWARM] %s failed: %s", sym, e)
    except Exception as e:
        _log.warning("[PREWARM] Module import failed: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    _auto_restart_bots()
    # Background prewarm (non-blocking)
    import threading
    threading.Thread(target=_startup_prewarm, daemon=True).start()
    yield
    for email, proc in _running_processes.items():
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

app = FastAPI(title="QuantX Deployer", lifespan=lifespan)

# ── Symbol cache ────────────────────────────────────────────────────────────
_symbol_cache: dict[str, dict] = {
    "SPY.US": {"found": True, "symbol": "SPY.US", "name": "SPDR S&P 500 ETF Trust", "exchange": "AMEX", "lot_size": 1, "currency": "USD"},
    "TQQQ.US": {"found": True, "symbol": "TQQQ.US", "name": "ProShares UltraPro QQQ", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "QQQ.US": {"found": True, "symbol": "QQQ.US", "name": "Invesco QQQ Trust", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "AAPL.US": {"found": True, "symbol": "AAPL.US", "name": "Apple Inc", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "MSFT.US": {"found": True, "symbol": "MSFT.US", "name": "Microsoft Corporation", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "META.US": {"found": True, "symbol": "META.US", "name": "Meta Platforms Inc", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "NVDA.US": {"found": True, "symbol": "NVDA.US", "name": "NVIDIA Corporation", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "TSLA.US": {"found": True, "symbol": "TSLA.US", "name": "Tesla Inc", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "AMZN.US": {"found": True, "symbol": "AMZN.US", "name": "Amazon.com Inc", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "GOOGL.US": {"found": True, "symbol": "GOOGL.US", "name": "Alphabet Inc Class A", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "SOXL.US": {"found": True, "symbol": "SOXL.US", "name": "Direxion Daily Semiconductor Bull 3X", "exchange": "NYSE", "lot_size": 1, "currency": "USD"},
    "SQQQ.US": {"found": True, "symbol": "SQQQ.US", "name": "ProShares UltraPro Short QQQ", "exchange": "NASDAQ", "lot_size": 1, "currency": "USD"},
    "GLD.US": {"found": True, "symbol": "GLD.US", "name": "SPDR Gold Shares", "exchange": "NYSE", "lot_size": 1, "currency": "USD"},
    "700.HK": {"found": True, "symbol": "700.HK", "name": "Tencent Holdings Ltd", "exchange": "HKEX", "lot_size": 100, "currency": "HKD"},
    "2800.HK": {"found": True, "symbol": "2800.HK", "name": "Tracker Fund of Hong Kong", "exchange": "HKEX", "lot_size": 500, "currency": "HKD"},
    "9988.HK": {"found": True, "symbol": "9988.HK", "name": "Alibaba Group Holding", "exchange": "HKEX", "lot_size": 100, "currency": "HKD"},
    "0005.HK": {"found": True, "symbol": "0005.HK", "name": "HSBC Holdings plc", "exchange": "HKEX", "lot_size": 400, "currency": "HKD"},
    "1299.HK": {"found": True, "symbol": "1299.HK", "name": "AIA Group Limited", "exchange": "HKEX", "lot_size": 200, "currency": "HKD"},
}

_executor = ThreadPoolExecutor(max_workers=4)

static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/api/debug-fmp")
async def debug_fmp():
    import requests as _req
    key = os.environ.get("FMP_API_KEY", "")
    if not key:
        return {"error": "FMP_API_KEY not set", "key_len": 0}
    results = {}
    tests = {
        "v3_1min": f"https://financialmodelingprep.com/api/v3/historical-chart/1min/AAPL?apikey={key}&limit=3",
        "v3_5min": f"https://financialmodelingprep.com/api/v3/historical-chart/5min/AAPL?apikey={key}&limit=3",
        "v3_30min": f"https://financialmodelingprep.com/api/v3/historical-chart/30min/AAPL?apikey={key}&limit=3",
        "v3_1hour": f"https://financialmodelingprep.com/api/v3/historical-chart/1hour/AAPL?apikey={key}&limit=3",
        "stable_1min": f"https://financialmodelingprep.com/stable/historical-chart/1min/AAPL?apikey={key}&limit=3",
        "stable_5min": f"https://financialmodelingprep.com/stable/historical-chart/5min/AAPL?apikey={key}&limit=3",
        "stable_30min": f"https://financialmodelingprep.com/stable/historical-chart/30min/AAPL?apikey={key}&limit=3",
        "stable_daily": f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&limit=3&apikey={key}",
        "v3_daily": f"https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey={key}&timeseries=3",
    }
    for name, url in tests.items():
        try:
            r = _req.get(url, timeout=10)
            d = r.json() if r.status_code == 200 else None
            bars = len(d) if isinstance(d, list) else (len(d.get("historical", [])) if isinstance(d, dict) and "historical" in d else 0)
            results[name] = {"status": r.status_code, "bars": bars}
        except Exception as e:
            results[name] = {"status": "error", "msg": str(e)[:60]}
    return {"key_set": True, "key_len": len(key), "tests": results}


@app.get("/api/debug-env")
async def debug_env():
    return {
        "FMP_API_KEY_set": bool(os.environ.get("FMP_API_KEY")),
        "FMP_API_KEY_length": len(os.environ.get("FMP_API_KEY", "")),
        "CENTRAL_API_URL": os.environ.get("CENTRAL_API_URL", "not set"),
        "HOSTING": os.environ.get("HOSTING", "not set"),
        "R2_ENDPOINT_set": bool(os.environ.get("R2_ENDPOINT_URL")),
    }


# ── Routes ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    fp = static_dir / "index.html"
    if fp.exists():
        return HTMLResponse(fp.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>QuantX Deployer</h1>")


@app.post("/api/login")
async def login(req: RegisterReq):
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        return {"found": False}
    return {
        "found": True,
        "email": student["email"],
        "name": student["name"],
        "has_credentials": bool(student.get("app_key")),
        "central_api_url": student.get("central_api_url", ""),
    }


@app.get("/api/me")
async def me(email: str = Query("")):
    email = email.lower().strip()
    if not email:
        return {"found": False}
    student = get_student(email)
    if not student:
        return {"found": False}
    return {
        "found": True,
        "email": student["email"],
        "name": student["name"],
        "has_credentials": bool(student.get("app_key")),
        "central_api_url": student.get("central_api_url", ""),
    }


@app.post("/api/download-script")
async def download_script(req: DownloadScriptReq):
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not found. Register first.")
    strat = {
        "strategy_id": req.strategy_id or f"{req.library_id}_DL",
        "strategy_name": req.library_id.replace("_", " ").title(),
        "symbol": req.symbol, "arena": req.arena.upper(),
        "timeframe": req.timeframe, "mode": "library",
        "library_id": req.library_id,
        "conditions": {"type": req.library_id, "params": req.params},
        "exit_rules": {}, "risk": req.risk or {"tp_pct": 0.5, "sl_pct": 0.3, "lots": 1},
        "is_active": True, "custom_script": "",
    }
    path = generate_master_bot(email, [strat], student)
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    from datetime import date
    header = f'#!/usr/bin/env python3\n# QuantX Strategy Bot - {req.library_id}\n# Symbol: {req.symbol} | Generated: {date.today()} | Student: {email}\n# Run: pip install longport httpx && python this_file.py\n\n'
    filename = f"quantx_{req.library_id.lower()}_{req.symbol.replace('.','_').lower()}_{date.today()}.py"
    return Response(content=header + content, media_type="text/plain",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


def _safe_json(row, key):
    """Safely parse a JSON column from sqlite3.Row."""
    try:
        rk = row.keys()
        if key not in rk:
            return None
        val = row[key]
        if not val or val == "NULL":
            return None
        return json.loads(val) if isinstance(val, str) else val
    except Exception:
        return None


@app.get("/api/strategies/{strategy_id}/detail")
async def strategy_detail(strategy_id: str, email: str = Query("")):
    email = email.lower().strip()
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM strategies WHERE strategy_id = ? AND email = ?", (strategy_id, email)).fetchone()
        if not row:
            raise HTTPException(404, "Strategy not found")
        rk = row.keys()
        return {
            "strategy_id": row["strategy_id"], "strategy_name": row["strategy_name"],
            "symbol": row["symbol"], "arena": row["arena"],
            "timeframe": row["timeframe"] if "timeframe" in rk else "1day",
            "conditions": _safe_json(row, "conditions_json") or {},
            "risk": _safe_json(row, "risk_json") or {},
            "is_active": bool(row["is_active"]),
            "mode": row["mode"] if "mode" in rk else "library",
            "library_id": row["library_id"] if "library_id" in rk else "",
            "allocation": float(row["allocation"]) if "allocation" in rk and row["allocation"] else 10000,
            "backtest_results": _safe_json(row, "backtest_results_json"),
            "live_results": _safe_json(row, "live_results_json"),
            "trade_log": _safe_json(row, "trade_log_json") or [],
            "created_at": row["created_at"] if "created_at" in rk else "",
        }
    except HTTPException:
        raise
    except Exception as e:
        _log.error("strategy_detail error: %s", e)
        raise HTTPException(500, f"Error loading strategy: {e}")
    finally:
        conn.close()


@app.put("/api/strategies/{strategy_id}/backtest-results")
async def save_backtest_results(strategy_id: str, body: dict):
    conn = get_db()
    try:
        conn.execute("UPDATE strategies SET backtest_results_json = ? WHERE strategy_id = ?",
                     (json.dumps(body), strategy_id))
        conn.commit()
    except Exception as e:
        _log.error("save_backtest_results: %s", e)
    finally:
        conn.close()
    return {"ok": True}


@app.put("/api/strategies/{strategy_id}/allocation")
async def update_allocation(strategy_id: str, body: dict):
    conn = get_db()
    try:
        conn.execute("UPDATE strategies SET allocation = ? WHERE strategy_id = ?", (body.get("allocation", 10000), strategy_id))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.post("/api/strategies/{strategy_id}/trade")
async def log_bot_trade(strategy_id: str, body: dict):
    """Called by running bot when a trade closes. Appends to trade_log, recalculates live_results."""
    conn = get_db()
    try:
        row = conn.execute("SELECT trade_log_json, allocation FROM strategies WHERE strategy_id = ?", (strategy_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Strategy not found")
        log = json.loads(row["trade_log_json"]) if row["trade_log_json"] else []
        log.append(body)
        alloc = float(row["allocation"]) if row["allocation"] else 10000
        # Recalculate live_results
        pnls = [t.get("pnl", 0) for t in log]
        total_pnl = sum(pnls)
        wins = sum(1 for p in pnls if p > 0)
        total_ret = total_pnl / alloc * 100 if alloc > 0 else 0
        wr = wins / len(pnls) * 100 if pnls else 0
        # Max drawdown from cumulative
        cum = 0; peak = 0; max_dd = 0
        for p in pnls:
            cum += p; peak = max(peak, cum); dd = (peak - cum) / alloc * 100 if alloc > 0 else 0; max_dd = max(max_dd, dd)
        live = {"total_return_pct": round(total_ret, 2), "win_rate_pct": round(wr, 1), "max_drawdown_pct": round(max_dd, 2), "total_trades": len(pnls), "total_pnl": round(total_pnl, 2)}
        conn.execute("UPDATE strategies SET trade_log_json = ?, live_results_json = ? WHERE strategy_id = ?",
                     (json.dumps(log), json.dumps(live), strategy_id))
        conn.commit()
        return {"ok": True, "live_results": live}
    finally:
        conn.close()


@app.post("/api/backtest/run")
async def backtest_run(body: BacktestReq):
    from api.backtest import fetch_ohlcv, run_backtest
    def _run():
        bars, source = fetch_ohlcv(body.symbol, body.timeframe, body.limit)
        result = run_backtest(bars, body.strategy, body.params, body.initial_capital,
                              body.commission_pct, body.slippage_pct)
        result["source"] = source
        result["symbol"] = body.symbol
        result["strategy"] = body.strategy
        result["commission_pct"] = body.commission_pct
        result["slippage_pct"] = body.slippage_pct
        return result
    try:
        result = await asyncio.get_event_loop().run_in_executor(_executor, _run)
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/backtest/optimize")
async def backtest_optimize(body: OptimizeReq):
    from api.backtest import fetch_ohlcv, run_optimization
    def _run():
        bars, source = fetch_ohlcv(body.symbol, body.timeframe, body.limit)
        result = run_optimization(bars, body.strategy, body.param_grid, body.initial_capital)
        result["source"] = source
        result["symbol"] = body.symbol
        return result
    try:
        result = await asyncio.get_event_loop().run_in_executor(_executor, _run)
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/backtest/run-script")
async def backtest_run_script(body: ScriptBacktestReq):
    from api.backtest import fetch_ohlcv, run_backtest_script
    def _run():
        bars, source = fetch_ohlcv(body.symbol, body.timeframe, body.limit)
        result = run_backtest_script(bars, body.script, body.initial_capital,
                                     params_override=body.params if body.params else None)
        result["source"] = source
        result["symbol"] = body.symbol
        result["strategy"] = "CUSTOM_SCRIPT"
        return result
    try:
        result = await asyncio.get_event_loop().run_in_executor(_executor, _run)
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/backtest/sweep-script")
async def backtest_sweep_script(body: SweepScriptReq):
    from api.backtest import fetch_ohlcv, run_backtest_script
    def _run():
        # Fetch data ONCE for all combos
        bars, source = fetch_ohlcv(body.symbol, body.timeframe, body.limit)
        results = []
        for combo in body.combos:
            try:
                r = run_backtest_script(bars, body.script, body.initial_capital,
                                        params_override=combo)
                r["params"] = combo
                r["source"] = source
                results.append(r)
            except Exception as e:
                results.append({"params": combo, "error": str(e)[:200]})
        # Sort by sharpe (non-error results first)
        results.sort(key=lambda x: x.get("metrics", {}).get("sharpe_ratio", -999), reverse=True)
        return {"results": results, "total": len(body.combos), "symbol": body.symbol}
    try:
        result = await asyncio.get_event_loop().run_in_executor(_executor, _run)
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/fundamentals/{symbol}")
async def fundamentals(symbol: str):
    from api.backtest import get_fundamentals, _fmp_symbol
    data = get_fundamentals(symbol)
    if not data:
        raise HTTPException(404, "Fundamentals not available")
    # Extract latest from metrics
    metrics = data.get("metrics", [{}])
    latest = metrics[0] if metrics else {}
    ttm = (data.get("metrics_ttm") or [{}])
    latest_ttm = ttm[0] if ttm else {}
    return {
        "symbol": symbol,
        "roe": latest.get("roe") or latest_ttm.get("roeTTM"),
        "roa": latest.get("roa") or latest_ttm.get("roaTTM"),
        "pe_ratio": latest.get("peRatio") or latest_ttm.get("peRatioTTM"),
        "debt_to_equity": latest.get("debtToEquity") or latest_ttm.get("debtToEquityTTM"),
        "current_ratio": latest.get("currentRatio") or latest_ttm.get("currentRatioTTM"),
        "revenue_growth": latest.get("revenueGrowth"),
        "earnings_yield": latest.get("earningsYield") or latest_ttm.get("earningsYieldTTM"),
        "dividend_yield": latest.get("dividendYield") or latest_ttm.get("dividendYieldTTM"),
        "price_to_book": latest.get("pbRatio") or latest_ttm.get("pbRatioTTM"),
        "free_cash_flow_yield": latest.get("freeCashFlowYield") or latest_ttm.get("freeCashFlowYieldTTM"),
    }


@app.post("/api/backtest/prewarm-bulk")
async def prewarm_bulk(request: Request):
    auth = request.headers.get("X-Instructor-Key", "")
    expected = os.environ.get("INSTRUCTOR_KEY", "quantx2025")
    if auth != expected:
        raise HTTPException(403, "Invalid instructor key")
    from api.backtest import prewarm_symbol, PREWARM_SYMBOLS
    import time as _time
    results = []
    cached = fetched = errors = 0
    t0 = _time.time()
    for sym in PREWARM_SYMBOLS:
        r = prewarm_symbol(sym, "1day")
        results.append(r)
        if r["status"] == "cached": cached += 1
        elif r["status"] == "fetched": fetched += 1
        else: errors += 1
        if r["status"] == "fetched":
            _time.sleep(0.5)  # Rate limit protection
    elapsed = round(_time.time() - t0, 1)
    return {"results": results, "cached": cached, "fetched": fetched, "errors": errors, "elapsed_seconds": elapsed}


@app.get("/api/backtest/cache-inventory")
async def cache_inventory():
    from api.backtest import r2_list_keys, load_from_r2, PREWARM_SYMBOLS
    keys = r2_list_keys()
    by_tf = {}
    for k in keys:
        if k.endswith("/meta.json"):
            continue
        parts = k.replace("/full.json", "").split("/")
        if len(parts) >= 2:
            sym = parts[0].replace("_", ".")
            tf = parts[1]
            if tf not in by_tf:
                by_tf[tf] = []
            by_tf[tf].append(sym)
    # Check missing priority
    cached_daily = set(by_tf.get("1day", []))
    missing = [s for s in PREWARM_SYMBOLS if s.replace(".", "_") not in {x.replace(".", "_") for x in cached_daily}]
    return {"total_keys": len(keys), "by_timeframe": {k: {"count": len(v), "symbols": v} for k, v in by_tf.items()}, "missing_priority": missing}


@app.post("/api/screen-now")
async def screen_now(req: ScreenNowReq):
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not found")
    from api.universe import get_universe
    from api.screener import run_screener
    universe = get_universe(req.bot_type, req.arena, req.custom_tickers)

    def _run():
        from longport.openapi import Config, QuoteContext
        cfg = Config(app_key=student["app_key"], app_secret=student["app_secret"], access_token=student["access_token"])
        ctx = QuoteContext(cfg)
        from api.config import DB_PATH
        return run_screener(ctx, req.bot_type, universe, DB_PATH, email, req.strategy_id)

    result = await asyncio.get_event_loop().run_in_executor(_executor, _run)
    return result


@app.get("/api/screener-results")
async def get_screener_results(email: str = Query(""), strategy_id: str = Query("")):
    from api.config import DB_PATH
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE IF NOT EXISTS screener_results (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, strategy_id TEXT, bot_type TEXT, symbol TEXT, score INTEGER, max_score INTEGER, shortlisted INTEGER, reasons TEXT, price REAL, run_at TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    rows = conn.execute("SELECT symbol,score,max_score,shortlisted,reasons,price,run_at,bot_type FROM screener_results WHERE email=? AND strategy_id=? ORDER BY score DESC", (email.lower().strip(), strategy_id)).fetchall()
    conn.close()
    return {"results": [dict(r) for r in rows], "count": len(rows), "shortlisted": sum(1 for r in rows if r["shortlisted"])}


@app.get("/api/approval-status")
async def approval_status(email: str = Query("")):
    email = email.lower().strip()
    if not email:
        return {"status": "unknown"}
    # Check central API
    student = get_student(email)
    central_url = (student.get("central_api_url", "") if student else "") or CENTRAL_API_URL
    if central_url:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(f"{central_url.rstrip('/')}/api/check-status?email={email}")
                if resp.status_code == 200:
                    return resp.json()
        except Exception:
            pass
    # Fallback: if student exists locally, assume approved
    if student:
        return {"status": "approved", "name": student.get("name", "")}
    return {"status": "unknown"}


@app.get("/api/indicators")
async def get_indicators():
    from api.indicators_library import INDICATOR_REGISTRY, CATEGORIES
    by_cat = {}
    for ind_id, meta in INDICATOR_REGISTRY.items():
        cat = meta["cat"]
        if cat not in by_cat:
            by_cat[cat] = []
        by_cat[cat].append({"id": ind_id, **meta})
    return {"categories": CATEGORIES, "indicators": by_cat, "total": len(INDICATOR_REGISTRY)}


@app.get("/api/config")
async def get_config():
    return {
        "central_api_url": CENTRAL_API_URL,
        "version": VERSION,
        "hosting": HOSTING,
    }


@app.get("/api/strategies-library")
async def strategies_library():
    """Serve strategy library — try central first, fallback to local copy."""
    # Try fetching from central
    students_dir = Path(__file__).parent.parent
    # Check if any student is registered to get their central URL
    conn = get_db()
    try:
        row = conn.execute("SELECT central_api_url FROM students LIMIT 1").fetchone()
        central_url = row["central_api_url"] if row and row["central_api_url"] else ""
    finally:
        conn.close()

    if not central_url:
        central_url = os.getenv("CENTRAL_API_URL", "")

    if central_url:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get(f"{central_url.rstrip('/')}/api/strategies-library")
                if resp.status_code == 200:
                    return resp.json()
        except Exception:
            pass  # Fall through to local copy

    # Fallback: read local copy
    local_path = students_dir / "strategies_library.json"
    if local_path.exists():
        return json.loads(local_path.read_text(encoding="utf-8"))
    return {"strategies": []}


@app.post("/api/register")
async def register(req: RegisterReq):
    email = req.email.lower().strip()
    central_url = req.central_api_url or CENTRAL_API_URL
    approval_status = "approved"  # Default: allow if no central

    if central_url and central_url != "http://localhost:8001":
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # Use self-register (creates pending if new)
                resp = await client.post(
                    f"{central_url.rstrip('/')}/api/self-register",
                    json={"email": email, "name": req.name},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    approval_status = data.get("status", "approved")
                else:
                    # Fallback to register-check
                    resp2 = await client.post(
                        f"{central_url.rstrip('/')}/api/register-check",
                        json={"email": email},
                    )
                    if resp2.status_code == 200:
                        approval_status = "approved"
                    else:
                        d = resp2.json()
                        return {"status": "rejected", "message": d.get("detail", "Not approved")}
        except Exception as e:
            _log.warning("Central API unreachable during register: %s — allowing offline", e)

    # Always store credentials locally (even if pending — ready when approved)
    name = req.name
    save_student(email, name, req.app_key, req.app_secret, req.access_token, central_url)

    if approval_status == "approved":
        return {"status": "registered", "email": email, "name": name}
    elif approval_status == "pending":
        return {"status": "pending", "email": email, "name": name,
                "message": "Registration pending instructor approval"}
    elif approval_status == "rejected":
        return {"status": "rejected", "email": email,
                "message": "Registration rejected. Contact your instructor."}
    else:
        return {"status": approval_status, "email": email}


@app.post("/api/strategy")
async def add_strategy(req: StrategyReq):
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not registered. Register first.")
    save_strategy(
        email, req.strategy_id, req.strategy_name, req.symbol, req.arena,
        req.timeframe, req.conditions, req.exit_rules, req.risk, req.is_active,
        mode=req.mode, library_id=req.library_id, custom_script=req.custom_script,
        broker=req.broker,
    )
    return {"status": "saved", "strategy_id": req.strategy_id}


@app.delete("/api/strategy/{strategy_id}")
async def remove_strategy(strategy_id: str):
    if not delete_strategy(strategy_id):
        raise HTTPException(404, "Strategy not found")
    return {"status": "deleted", "strategy_id": strategy_id}


@app.put("/api/strategy/{strategy_id}/toggle")
async def toggle_strat(strategy_id: str, active: bool = Query(True)):
    toggle_strategy(strategy_id, active)
    return {"status": "toggled", "strategy_id": strategy_id, "is_active": active}


@app.get("/api/strategies/{email}")
async def list_strategies(email: str):
    email = email.lower().strip()
    return {"strategies": get_strategies(email)}


def _cancel_student_orders(student: dict) -> int:
    """Cancel all open LongPort orders for a student. Returns count cancelled."""
    logger = _logging.getLogger("quantx-deployer")
    try:
        from longport.openapi import Config, TradeContext, OrderStatus
        cfg = Config(
            app_key=student["app_key"],
            app_secret=student["app_secret"],
            access_token=student["access_token"],
        )
        ctx = TradeContext(cfg)
        orders = ctx.today_orders()
        cancelled = 0
        for o in orders:
            status_name = str(o.status).split(".")[-1]
            if status_name in ("New", "NotReported", "PendingSubmit"):
                try:
                    ctx.cancel_order(str(o.order_id))
                    cancelled += 1
                except Exception:
                    pass
        logger.info("[DEPLOY] Cancelled %d old open orders", cancelled)
        return cancelled
    except Exception as e:
        msg = str(e)
        if "connections limitation" in msg.lower() or "limit" in msg.lower():
            logger.warning("[DEPLOY] LongPort connection limit hit — skipping order cancel, proceeding with deploy")
        else:
            logger.warning("[DEPLOY] Could not cancel old orders: %s", e)
        return 0


@app.post("/api/deploy")
async def deploy(req: DeployReq):
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not registered")

    # Stop existing process first
    if email in _running_processes:
        _stop_process(email)

    # Cancel all existing open orders on LongPort (clean slate)
    old_cancelled = _cancel_student_orders(student)

    # Get active strategies
    strategies = get_strategies(email, active_only=True)
    if not strategies:
        raise HTTPException(400, "No active strategies to deploy")

    # Resolve central API URL with fallback
    central_url = student.get("central_api_url", "").strip()
    if not central_url:
        central_url = os.getenv("CENTRAL_API_URL", "http://localhost:8001")
    student["central_api_url"] = central_url

    _log.info("Deploying with central API: %s", central_url)

    # Split strategies by broker
    lp_strats = [s for s in strategies if s.get("broker", "longport") != "ibkr"]
    ibkr_strats = [s for s in strategies if s.get("broker") == "ibkr"]

    # If there are IBKR strategies, launch IBKR bot too
    if ibkr_strats:
        ibkr_cfg = get_ibkr_config(email) or {"host": "127.0.0.1", "port": 7497, "client_id": 1}
        ibkr_script = generate_ibkr_bot(email, ibkr_strats, student, ibkr_cfg)
        email_safe = email.replace("@", "_at_").replace(".", "_")
        ibkr_log = str(LOGS_DIR / f"{email_safe}_ibkr_master.log")
        try:
            ibkr_proc = _launch_bot(ibkr_script, ibkr_log)
            _log.info("[DEPLOY] IBKR bot PID: %s for %d strategies", ibkr_proc.pid, len(ibkr_strats))
        except Exception as e:
            _log.error("[DEPLOY] IBKR bot launch failed: %s", e)

    # Use LongPort strategies for the main bot (or all if no broker split)
    deploy_strats = lp_strats if lp_strats else strategies
    if not lp_strats and ibkr_strats:
        # All strategies are IBKR — no LongPort bot needed
        return {
            "status": "deployed",
            "broker": "ibkr",
            "strategies_count": len(ibkr_strats),
            "central_api_url": central_url,
        }

    # Generate LongPort master bot
    script_path = generate_master_bot(email, deploy_strats, student)

    # Prepare log file path and ensure logs directory exists
    email_safe = email.replace("@", "_at_").replace(".", "_")
    logs_dir = Path(script_path).parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = str(logs_dir / f"{email_safe}_master.log")

    _log.info("[DEPLOY] Running: %s %s | Log: %s", PYTHON_EXE, script_path, log_path)

    try:
        proc = _launch_bot(script_path, log_path)
        _running_processes[email] = proc
        save_process(email, proc.pid, "running", script_path, log_path)
        _log.info("[DEPLOY] PID: %s", proc.pid)

        # Wait 3 seconds to check for immediate crash
        time.sleep(3)
        exit_code = proc.poll()
        if exit_code is not None:
            # Bot crashed on startup
            log_fh.close()
            del _running_processes[email]
            update_process_status(email, "error", f"Crashed on startup. Exit code: {exit_code}")
            # Read log excerpt
            log_excerpt = []
            try:
                log_excerpt = Path(log_path).read_text(encoding="utf-8", errors="replace").splitlines()[-20:]
            except Exception:
                pass
            return {
                "status": "error",
                "error": f"Bot crashed on startup (exit code {exit_code})",
                "log_excerpt": log_excerpt,
                "pid": proc.pid,
            }

        return {
            "status": "deployed",
            "pid": proc.pid,
            "script": script_path,
            "log_path": log_path,
            "strategies_count": len(strategies),
            "central_api_url": central_url,
            "old_orders_cancelled": old_cancelled,
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to launch bot: {e}")


@app.post("/api/stop")
async def stop(req: StopReq):
    email = req.email.lower().strip()
    if email not in _running_processes:
        raise HTTPException(404, "No running bot found for this email")
    _stop_process(email)
    return {"status": "stopped"}


@app.post("/api/restart")
async def restart(req: DeployReq):
    email = req.email.lower().strip()
    if email in _running_processes:
        _stop_process(email)
    return await deploy(req)


@app.post("/api/ibkr-config")
async def set_ibkr_config(req: IBKRConfigReq):
    email = req.email.lower().strip()
    save_ibkr_config(email, req.host, req.port, req.client_id)
    return {"status": "saved", "host": req.host, "port": req.port, "client_id": req.client_id}


@app.get("/api/ibkr-config")
async def get_ibkr_config_endpoint(email: str):
    cfg = get_ibkr_config(email.lower().strip())
    if not cfg:
        return {"configured": False}
    return {"configured": True, **cfg}


@app.get("/api/status/{email}")
async def status(email: str):
    email = email.lower().strip()
    proc_info = get_latest_process(email)
    is_running = False
    pid = None
    bot_status = "stopped"

    if email in _running_processes:
        proc = _running_processes[email]
        if proc.poll() is None:
            is_running = True
            pid = proc.pid
            bot_status = "running"
        else:
            # Process ended unexpectedly
            exit_code = proc.returncode
            del _running_processes[email]
            bot_status = "error" if exit_code != 0 else "stopped"
            update_process_status(email, bot_status, f"Exit code: {exit_code}")

    strategies = get_strategies(email)
    trades = get_trades(email)

    # Calculate per-strategy PnL
    strat_pnl = defaultdict(float)
    for t in trades:
        strat_pnl[t["strategy_id"]] += t["pnl"]

    # Read recent log lines
    email_safe = email.replace("@", "_at_").replace(".", "_")
    log_path = Path(__file__).parent.parent / "logs" / f"{email_safe}_master.log"
    recent_logs = []
    if log_path.exists():
        try:
            recent_logs = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-5:]
        except Exception:
            pass

    return {
        "email": email,
        "is_running": is_running,
        "pid": pid,
        "bot_status": bot_status,
        "process": proc_info,
        "strategies": [
            {**s, "total_pnl": round(strat_pnl.get(s["strategy_id"], 0), 4)}
            for s in strategies
        ],
        "total_pnl": round(sum(strat_pnl.values()), 4),
        "total_trades": len(trades),
        "recent_logs": recent_logs,
    }


@app.get("/api/logs/{email}/{strategy_id}")
async def strategy_logs(email: str, strategy_id: str, lines: int = Query(50, ge=1, le=500)):
    email = email.lower().strip()
    email_safe = email.replace("@", "_at_").replace(".", "_")
    safe_sid = strategy_id.replace("/", "_")
    logs_dir = Path(__file__).parent.parent / "logs"
    # Search for log file in order of specificity
    candidates = [
        f"{email_safe}_{safe_sid}.log",       # strategy-specific
        f"{email_safe}_master.log",            # LongPort master
        f"{email_safe}_ibkr_master.log",       # IBKR master
    ]
    for fname in candidates:
        log_path = logs_dir / fname
        if log_path.exists():
            all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            return {"lines": all_lines[-lines:], "total": len(all_lines),
                    "strategy_id": strategy_id, "filename": fname}
    return {"lines": ["No log file found yet. Deploy a bot to start logging."],
            "total": 0, "strategy_id": strategy_id, "filename": ""}


@app.get("/api/logs/{email}")
async def logs(email: str, lines: int = Query(50, ge=1, le=500)):
    email = email.lower().strip()
    email_safe = email.replace("@", "_at_").replace(".", "_")
    logs_dir = Path(__file__).parent.parent / "logs"
    for fname in [f"{email_safe}_master.log", f"{email_safe}_ibkr_master.log"]:
        log_path = logs_dir / fname
        if log_path.exists():
            all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            return {"lines": all_lines[-lines:], "total": len(all_lines), "filename": fname}
    return {"lines": [], "total": 0, "filename": ""}


@app.get("/api/trades/{email}")
async def trades_list(email: str):
    email = email.lower().strip()
    trades = get_trades(email)
    # Group by strategy_id
    by_strategy = defaultdict(list)
    for t in trades:
        by_strategy[t.get("strategy_id", "unknown")].append(t)
    return {"trades": trades, "by_strategy": dict(by_strategy)}


@app.post("/api/trade")
async def trade_report(req: TradeReq):
    email = req.email.lower().strip()
    # Log locally
    cum_pnl = log_trade(email, req.strategy_id, req.symbol, req.side, req.price, req.qty, req.pnl)

    # Forward to central API
    student = get_student(email)
    if student and student.get("central_api_url"):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"{student['central_api_url'].rstrip('/')}/api/trade",
                    json={
                        "email": email,
                        "student_name": student.get("name", ""),
                        "strategy_id": req.strategy_id,
                        "symbol": req.symbol,
                        "side": req.side,
                        "price": req.price,
                        "qty": req.qty,
                        "pnl": req.pnl,
                    },
                )
        except Exception:
            pass  # Don't fail local trade log if central is unreachable

    return {"status": "recorded", "cumulative_pnl": cum_pnl}


def _fetch_symbol_lp(symbol: str, email: str) -> dict:
    """Blocking LongPort symbol lookup — runs in thread pool."""
    student = get_student(email) if email else None
    if not student:
        return {"found": False, "error": "Register to search unlisted symbols."}
    try:
        from longport.openapi import Config, QuoteContext
        cfg = Config(app_key=student["app_key"], app_secret=student["app_secret"], access_token=student["access_token"])
        ctx = QuoteContext(cfg)
        result = ctx.static_info([symbol])
        if result and len(result) > 0:
            info = result[0]
            return {
                "found": True, "symbol": info.symbol,
                "name": getattr(info, "name_en", "") or getattr(info, "name_cn", "") or str(info.symbol),
                "exchange": getattr(info, "exchange", ""),
                "lot_size": getattr(info, "lot_size", 0),
                "currency": getattr(info, "currency", ""),
            }
        return {"found": False, "error": f"Symbol {symbol} not found"}
    except Exception as e:
        return {"found": False, "error": str(e)}


@app.get("/api/symbol-search")
async def symbol_search(query: str = Query(""), email: str = Query("")):
    symbol = query.upper().strip()
    if not symbol:
        return {"found": False, "error": "Empty query"}
    if "." not in symbol:
        symbol = f"{symbol}.US"
    # Cache hit = instant
    if symbol in _symbol_cache:
        return _symbol_cache[symbol]
    # LongPort lookup in thread pool (non-blocking)
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_executor, _fetch_symbol_lp, symbol, email.lower().strip())
    if result.get("found"):
        _symbol_cache[symbol] = result
    return result


@app.post("/api/test-connection")
async def test_connection(req: DeployReq):
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not registered")
    try:
        from longport.openapi import Config, QuoteContext
        lp_config = Config(
            app_key=student["app_key"],
            app_secret=student["app_secret"],
            access_token=student["access_token"],
        )
        quote_ctx = QuoteContext(lp_config)
        quotes = quote_ctx.quote(["700.HK"])
        if quotes:
            q = quotes[0]
            return {
                "ok": True,
                "message": "Connected to LongPort successfully",
                "test_quote": {"symbol": "700.HK", "price": float(q.last_done)},
            }
        return {"ok": True, "message": "Connected but no quote data returned", "test_quote": None}
    except Exception as e:
        return {"ok": False, "message": f"Connection failed: {e}"}


@app.post("/api/settings")
async def update_settings(req: SettingsReq):
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not registered")
    conn = get_db()
    try:
        conn.execute(
            "UPDATE students SET central_api_url = ? WHERE email = ?",
            (req.central_api_url, email),
        )
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "central_api_url": req.central_api_url}


@app.post("/api/validate-script")
async def validate_script(req: ValidateScriptReq):
    script = req.script.strip()
    if not script:
        return {"valid": False, "stage": "syntax", "error": "Script is empty"}

    # Stage 1 — Syntax check
    # Wrap in a class to validate as a method body
    wrapper = "class _TestRunner:\n"
    for line in script.split("\n"):
        wrapper += "    " + line + "\n"
    try:
        ast.parse(wrapper)
    except SyntaxError as e:
        # Adjust line number to account for the wrapper class line
        lineno = (e.lineno - 1) if e.lineno else 0
        return {"valid": False, "stage": "syntax", "error": f"line {lineno}: {e.msg}"}

    # Stage 2 — Sandbox execution test
    random.seed(42)
    closes = deque(maxlen=500)
    highs = deque(maxlen=500)
    lows = deque(maxlen=500)
    vols = deque(maxlen=500)
    for i in range(200):
        c = 100 + i * 0.01 + random.random() * 0.5
        closes.append(c)
        highs.append(c + random.random() * 0.2)
        lows.append(c - random.random() * 0.2)
        vols.append(1000000 + random.randint(0, 500000))

    # Build a minimal stub with helper methods
    stub_code = """
import math
from collections import deque

class _StubRunner:
    def ema(self, data, period):
        d = list(data)
        if len(d) < period:
            return None
        mult = 2 / (period + 1)
        val = sum(d[:period]) / period
        for p in d[period:]:
            val = (p - val) * mult + val
        return val

    def sma(self, data, period):
        d = list(data)
        if len(d) < period:
            return None
        return sum(d[-period:]) / period

    def rsi(self, data, period):
        d = list(data)
        if len(d) < period + 1:
            return None
        gains, losses = 0.0, 0.0
        for i in range(-period, 0):
            diff = d[i] - d[i - 1]
            if diff > 0:
                gains += diff
            else:
                losses -= diff
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def atr(self, highs, lows, closes, period):
        h, l, c = list(highs), list(lows), list(closes)
        if len(c) < period + 1:
            return None
        trs = []
        for i in range(1, len(c)):
            tr = max(h[i] - l[i], abs(h[i] - c[i-1]), abs(l[i] - c[i-1]))
            trs.append(tr)
        if len(trs) < period:
            return None
        return sum(trs[-period:]) / period

"""
    stub_code += "    " + script.replace("\n", "\n    ") + "\n"

    try:
        ns = {}
        exec(stub_code, ns)
        runner = ns["_StubRunner"]()
    except Exception as e:
        return {"valid": False, "stage": "sandbox", "error": f"Failed to instantiate: {e}"}

    signals_sample = []
    for tick in range(10):
        window = tick * 20 + 20  # 20, 40, 60, ... 200
        c_slice = deque(list(closes)[:window], maxlen=500)
        h_slice = deque(list(highs)[:window], maxlen=500)
        l_slice = deque(list(lows)[:window], maxlen=500)
        v_slice = deque(list(vols)[:window], maxlen=500)
        try:
            val = runner.get_signal(c_slice, h_slice, l_slice, v_slice)
        except Exception as e:
            return {"valid": False, "stage": "sandbox", "error": str(e)}
        if val not in ("buy", "sell", None):
            return {
                "valid": False,
                "stage": "return_value",
                "error": f"get_signal returned {val!r} — must return 'buy', 'sell', or None",
            }
        signals_sample.append(val)

    return {"valid": True, "stage": "passed", "signals_sample": signals_sample[-5:]}


# ── Helpers ─────────────────────────────────────────────────────────────────

def _stop_process(email: str):
    proc = _running_processes.get(email)
    if proc is None:
        return
    try:
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    _running_processes.pop(email, None)
    update_process_status(email, "stopped")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("api.main:app", host="0.0.0.0", port=port, app_dir=str(Path(__file__).parent.parent))
