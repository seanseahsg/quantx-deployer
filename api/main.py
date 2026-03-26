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

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
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
)
from api.generate import generate_master_bot

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


class ValidateScriptReq(BaseModel):
    script: str


class DeployReq(BaseModel):
    email: str


class StopReq(BaseModel):
    email: str


class SettingsReq(BaseModel):
    email: str
    central_api_url: str = ""


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    _auto_restart_bots()
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

static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ── Routes ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    fp = static_dir / "index.html"
    if fp.exists():
        return HTMLResponse(fp.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>QuantX Deployer</h1>")


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
    # Validate against central API
    if req.central_api_url:
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                resp = await client.post(
                    f"{req.central_api_url.rstrip('/')}/api/register-check",
                    json={"email": email},
                )
                if resp.status_code != 200:
                    data = resp.json()
                    raise HTTPException(403, data.get("detail", "Email not pre-approved"))
                central_data = resp.json()
            except httpx.HTTPError as e:
                raise HTTPException(502, f"Cannot reach central API: {e}")
    else:
        central_data = {"name": req.name}

    name = central_data.get("name") or req.name
    save_student(email, name, req.app_key, req.app_secret, req.access_token, req.central_api_url)
    return {"status": "registered", "email": email, "name": name}


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

    # Generate master bot
    script_path = generate_master_bot(email, strategies, student)

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
    log_path = Path(__file__).parent.parent / "logs" / f"{email_safe}_{safe_sid}.log"
    if not log_path.exists():
        return {"lines": [], "total": 0, "strategy_id": strategy_id}
    all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    return {"lines": all_lines[-lines:], "total": len(all_lines), "strategy_id": strategy_id}


@app.get("/api/logs/{email}")
async def logs(email: str, lines: int = Query(50, ge=1, le=500)):
    email = email.lower().strip()
    email_safe = email.replace("@", "_at_").replace(".", "_")
    log_path = Path(__file__).parent.parent / "logs" / f"{email_safe}_master.log"
    if not log_path.exists():
        return {"lines": [], "total": 0}
    all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    return {"lines": all_lines[-lines:], "total": len(all_lines)}


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


@app.get("/api/symbol-search")
async def symbol_search(query: str = Query(""), email: str = Query("")):
    email = email.lower().strip()
    symbol = query.upper().strip()
    if not symbol:
        return {"found": False, "error": "Empty query"}
    if "." not in symbol:
        symbol = f"{symbol}.US"

    student = get_student(email) if email else None
    if not student:
        return {"found": False, "error": "Student not registered. Register first."}

    try:
        from longport.openapi import Config, QuoteContext
        cfg = Config(
            app_key=student["app_key"],
            app_secret=student["app_secret"],
            access_token=student["access_token"],
        )
        ctx = QuoteContext(cfg)
        result = ctx.static_info([symbol])
        if result and len(result) > 0:
            info = result[0]
            return {
                "found": True,
                "symbol": info.symbol,
                "name": getattr(info, "name_en", "") or getattr(info, "name_cn", "") or str(info.symbol),
                "exchange": getattr(info, "exchange", ""),
                "lot_size": getattr(info, "lot_size", 0),
                "currency": getattr(info, "currency", ""),
            }
        return {"found": False, "error": f"Symbol {symbol} not found"}
    except Exception as e:
        return {"found": False, "error": str(e)}


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
