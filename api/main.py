"""QuantX Deployer — Student trading bot manager. Runs on VPS or Railway."""

import os
import sys
import ast
import secrets
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
from fastapi import FastAPI, HTTPException, Query, Request, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from api.config import (
    CENTRAL_API_URL, HOSTING, VERSION, BOTS_DIR, LOGS_DIR, PYTHON_EXE, DB_PATH,
)
from api.database import (
    get_db, save_student, get_student, save_strategy, get_strategies,
    delete_strategy, toggle_strategy, save_process, update_process_status,
    get_latest_process, log_trade, get_trades, decrypt,
    save_ibkr_config, get_ibkr_config,
    get_broker_accounts, get_broker_account, save_broker_account,
    update_broker_account_status, delete_broker_account, get_broker_credentials,
)
# Dual-mode init: PostgreSQL when DATABASE_URL is set, SQLite otherwise.
from api.db_postgres import init_db, USE_POSTGRES
from api.generate import (generate_master_bot, generate_ibkr_bot, generate_simple_lp_bot,
                          generate_simple_ibkr_bot, generate_ibkr_bot_prod,
                          generate_lp_master_bot, library_id_to_conditions)

_log = _logging.getLogger("quantx-deployer")


# ── LongPort connection pool ─────────────────────────────────────────────────
# Previously every API call built a fresh QuoteContext / TradeContext. LongPort
# enforces a per-key connection limit, and creating a context takes ~1s of
# handshake, so both the UX and the rate limits suffered under multi-student
# load. The pool below keys contexts by a hash of the credentials so each
# student reuses the same ctx for up to LP_POOL_TTL seconds.
#
# LongPort support has confirmed a single QuoteContext/TradeContext is safe to
# share across threads. Each student gets their own ctx so credential isolation
# is preserved.
import threading as _lp_threading  # noqa: E402  (distinct name to avoid shadowing)
import hashlib as _lp_hashlib      # noqa: E402

_lp_quote_pool: dict = {}           # cred_hash -> QuoteContext
_lp_trade_pool: dict = {}           # cred_hash -> TradeContext
_lp_pool_last_used: dict = {}       # cred_hash -> last-used epoch seconds
_lp_pool_lock = _lp_threading.Lock()

LP_POOL_TTL = 300  # seconds; after this, reuse stops and a fresh ctx is built


def _lp_cred_hash(app_key: str, app_secret: str, access_token: str) -> str:
    return _lp_hashlib.sha256(
        f"{app_key}:{app_secret}:{access_token}".encode()
    ).hexdigest()[:16]


def _lp_build_config(app_key: str, app_secret: str, access_token: str):
    """Config with optional LONGBRIDGE_LOG_PATH wired in (LP-requested)."""
    from longport.openapi import Config
    kwargs = dict(app_key=app_key, app_secret=app_secret, access_token=access_token)
    log_path = os.environ.get("LONGBRIDGE_LOG_PATH", "")
    if log_path:
        kwargs["log_path"] = log_path
    return Config(**kwargs)


def get_lp_quote_ctx(app_key: str, app_secret: str, access_token: str):
    """Return a reusable QuoteContext for these credentials."""
    from longport.openapi import QuoteContext
    key = _lp_cred_hash(app_key, app_secret, access_token)
    now = time.time()
    with _lp_pool_lock:
        if key in _lp_quote_pool and (now - _lp_pool_last_used.get(key, 0)) < LP_POOL_TTL:
            _lp_pool_last_used[key] = now
            return _lp_quote_pool[key]
        # Stale or absent -- drop and rebuild
        _lp_quote_pool.pop(key, None)
        ctx = QuoteContext(_lp_build_config(app_key, app_secret, access_token))
        _lp_quote_pool[key] = ctx
        _lp_pool_last_used[key] = now
        _log.info("[LP-pool] Created QuoteContext (quote_pool=%d)", len(_lp_quote_pool))
        return ctx


def get_lp_trade_ctx(app_key: str, app_secret: str, access_token: str):
    """Return a reusable TradeContext for these credentials."""
    from longport.openapi import TradeContext
    key = _lp_cred_hash(app_key, app_secret, access_token) + "_trade"
    now = time.time()
    with _lp_pool_lock:
        if key in _lp_trade_pool and (now - _lp_pool_last_used.get(key, 0)) < LP_POOL_TTL:
            _lp_pool_last_used[key] = now
            return _lp_trade_pool[key]
        _lp_trade_pool.pop(key, None)
        ctx = TradeContext(_lp_build_config(app_key, app_secret, access_token))
        _lp_trade_pool[key] = ctx
        _lp_pool_last_used[key] = now
        _log.info("[LP-pool] Created TradeContext (trade_pool=%d)", len(_lp_trade_pool))
        return ctx


def cleanup_lp_pool() -> int:
    """Drop contexts untouched for > 2x TTL. Returns number removed. Safe to
    call from a periodic task -- not wired to any scheduler by default."""
    now = time.time()
    dropped = 0
    with _lp_pool_lock:
        stale = [k for k, t in _lp_pool_last_used.items() if (now - t) > LP_POOL_TTL * 2]
        for k in stale:
            if _lp_quote_pool.pop(k, None) is not None:
                dropped += 1
            if _lp_trade_pool.pop(k, None) is not None:
                dropped += 1
            _lp_pool_last_used.pop(k, None)
    if dropped:
        _log.info("[LP-pool] cleanup removed %d stale contexts", dropped)
    return dropped


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
    broker: str = "all"


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
    email: str = ""
    use_ibkr: bool = False
    skip_cache: bool = False
    broker_hint: str = ""


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
    email: str = ""
    use_ibkr: bool = False
    skip_cache: bool = False
    broker_hint: str = ""


class SweepScriptReq(BaseModel):
    symbol: str = "TQQQ.US"
    timeframe: str = "1day"
    script: str = ""
    initial_capital: float = 10000
    limit: int = 1260
    combos: list = []
    stop_loss_pct: float = 0.0
    take_profit_pct: float = 0.0
    email: str = ""


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
@app.get("/api/health")
async def health():
    from datetime import datetime as _dt
    return {
        "ok": True,
        "version": VERSION,
        "hosting": HOSTING,
        "central_url": CENTRAL_API_URL,
        "timestamp": _dt.utcnow().isoformat(),
        "architecture": {
            "backtest": "railway" if CENTRAL_API_URL else "local",
            "trading": "local",
        },
        "auth_mode": "postgres" if USE_POSTGRES else "local",
    }


# ══════════════════════════════════════════════════════════════════════════════
# AUTHENTICATION
# ══════════════════════════════════════════════════════════════════════════════
# Two modes:
#   Local/SQLite (no DATABASE_URL): middleware is a no-op; existing flows unchanged
#   PostgreSQL/Railway: /api/* (except /api/auth/* and /api/health) requires JWT

_PUBLIC_API_PREFIXES = ("/api/auth/", "/api/health", "/api/debug",
                        "/api/options/share",                   # share links viewable without auth
                        "/api/options/cache/prewarm-popular",   # gated by ADMIN_PIN, not JWT
                        "/api/options/cache/repair")            # gated by ADMIN_PIN, not JWT


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Require JWT on /api/* routes when running in PostgreSQL mode."""
    if not USE_POSTGRES:
        return await call_next(request)
    path = request.url.path
    if not path.startswith("/api/"):
        return await call_next(request)
    if any(path.startswith(p) for p in _PUBLIC_API_PREFIXES):
        return await call_next(request)
    from api.auth import get_current_user
    user = get_current_user(request)
    if not user:
        from fastapi.responses import JSONResponse
        return JSONResponse({"detail": "Not authenticated"}, status_code=401)
    # Stash on request state so handlers can access without re-parsing
    request.state.user = user
    return await call_next(request)


class _RegisterBody(BaseModel):
    email: str
    password: str
    name: Optional[str] = None


class _LoginBody(BaseModel):
    email: str
    password: str


@app.post("/api/auth/register")
async def auth_register(body: _RegisterBody, response: Response):
    """Create a user + return JWT. PostgreSQL only."""
    if not USE_POSTGRES:
        raise HTTPException(400, "Registration requires PostgreSQL (DATABASE_URL unset)")
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    from api.auth import hash_password, create_token, COOKIE_NAME
    from api.db_postgres import get_conn
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users (email, name, password_hash, role, last_login)
            VALUES (%s, %s, %s, 'student', NOW())
            RETURNING id, email, name, role
        """, (body.email.lower().strip(), body.name, hash_password(body.password)))
        row = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            raise HTTPException(409, "Email already registered")
        raise HTTPException(500, f"Registration failed: {e}")
    finally:
        cur.close()
        conn.close()
    token = create_token(row["id"], row["email"], row["role"], row["name"])
    response.set_cookie(COOKIE_NAME, token, httponly=True, samesite="lax",
                        max_age=7 * 24 * 3600, secure=bool(os.environ.get("RAILWAY_ENVIRONMENT")))
    return {"token": token, "user": dict(row)}


@app.post("/api/auth/login")
async def auth_login(body: _LoginBody, response: Response):
    if not USE_POSTGRES:
        raise HTTPException(400, "Login requires PostgreSQL (DATABASE_URL unset)")
    from api.auth import verify_password, create_token, COOKIE_NAME
    from api.db_postgres import get_conn
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, email, name, role, password_hash FROM users WHERE email = %s
    """, (body.email.lower().strip(),))
    row = cur.fetchone()
    if not row or not verify_password(body.password, row["password_hash"]):
        cur.close()
        conn.close()
        raise HTTPException(401, "Invalid email or password")
    cur.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (row["id"],))
    conn.commit()
    cur.close()
    conn.close()
    token = create_token(row["id"], row["email"], row["role"], row["name"])
    response.set_cookie(COOKIE_NAME, token, httponly=True, samesite="lax",
                        max_age=7 * 24 * 3600, secure=bool(os.environ.get("RAILWAY_ENVIRONMENT")))
    user = {k: row[k] for k in ("id", "email", "name", "role")}
    return {"token": token, "user": user}


@app.post("/api/auth/logout")
async def auth_logout(response: Response):
    from api.auth import COOKIE_NAME
    response.delete_cookie(COOKIE_NAME)
    return {"ok": True}


@app.get("/api/auth/me")
async def auth_me(request: Request):
    from api.auth import get_current_user
    user = get_current_user(request)
    if not user:
        # In SQLite mode there's no user; return a synthetic "local" identity
        if not USE_POSTGRES:
            return {"user": None, "mode": "local"}
        raise HTTPException(401, "Not authenticated")
    return {"user": {k: user.get(k) for k in ("user_id", "email", "name", "role")},
            "mode": "postgres"}


# ══════════════════════════════════════════════════════════════════════════════
# LONGPORT OAUTH2
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/auth/longport/start")
async def longport_start(request: Request):
    """Begin OAuth2 flow: return the LongPort authorize URL for the frontend to open."""
    if not USE_POSTGRES:
        raise HTTPException(400, "LongPort OAuth requires PostgreSQL")
    from api.auth import get_current_user
    from api.longport_oauth import generate_pkce, get_authorize_url, save_oauth_state
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    verifier, challenge = generate_pkce()
    state = secrets.token_urlsafe(24)
    save_oauth_state(user["user_id"], state, verifier)
    return {"auth_url": get_authorize_url(state, challenge), "state": state}


@app.get("/callback/longport")
async def longport_callback(code: Optional[str] = None, state: Optional[str] = None,
                            error: Optional[str] = None):
    """Exchange the authorization code for tokens. No /api/ prefix -- LongPort's
    registered redirect_uri points here directly."""
    from fastapi.responses import RedirectResponse
    if error:
        return RedirectResponse(url=f"/#/brokers?oauth_error={error}")
    if not code or not state:
        return RedirectResponse(url="/#/brokers?oauth_error=missing_code_or_state")
    if not USE_POSTGRES:
        return RedirectResponse(url="/#/brokers?oauth_error=postgres_required")
    from api.longport_oauth import consume_oauth_state, exchange_code_for_tokens, store_tokens
    row = consume_oauth_state(state)
    if not row:
        return RedirectResponse(url="/#/brokers?oauth_error=invalid_or_expired_state")
    try:
        tokens = exchange_code_for_tokens(code, row["code_verifier"])
        store_tokens(row["user_id"], tokens)
    except Exception as e:
        log = _logging.getLogger("quantx-oauth-callback")
        log.exception("LongPort token exchange failed")
        return RedirectResponse(url=f"/#/brokers?oauth_error=exchange_failed&msg={str(e)[:80]}")
    return RedirectResponse(url="/#/brokers?connected=longport")


@app.get("/api/auth/longport/status")
async def longport_status(request: Request):
    from api.auth import get_current_user
    from api.longport_oauth import get_token_status
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    return get_token_status(user["user_id"])


@app.delete("/api/auth/longport/disconnect")
async def longport_disconnect(request: Request):
    from api.auth import get_current_user
    from api.longport_oauth import disconnect
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    disconnect(user["user_id"])
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
    from api.backtest import run_backtest
    from api.data_manager import fetch_bars_waterfall_sync
    def _run():
        # Get credentials if email provided, respecting broker_hint
        hint = (body.broker_hint or "").lower()
        ibkr_cfg = None
        if body.email and (body.use_ibkr or hint == "ibkr"):
            ibkr_cfg = get_ibkr_config(body.email.lower().strip())
        lp_creds = None
        if body.email and hint != "yahoo":
            student = get_student(body.email.lower().strip())
            if student and student.get("app_key"):
                lp_creds = {"app_key": student["app_key"],
                            "app_secret": student["app_secret"],
                            "access_token": student["access_token"]}
        # If user explicitly chose Yahoo, skip broker data sources
        if hint == "yahoo":
            ibkr_cfg = None
            lp_creds = None
        # Waterfall fetch
        data = fetch_bars_waterfall_sync(
            symbol=body.symbol, timeframe=body.timeframe, limit=body.limit,
            db_path=str(DB_PATH), ibkr_config=ibkr_cfg, lp_credentials=lp_creds,
            skip_cache=body.skip_cache)
        if data["error"]:
            return {"status": "error", "message": data["error"],
                    "source": data["source"], "source_message": data["source_message"]}
        bars = data["bars"]
        if len(bars) < 50:
            return {"status": "error", "message": f"Only {len(bars)} bars available, need 50+.",
                    "source": data["source"]}
        result = run_backtest(bars, body.strategy, body.params, body.initial_capital,
                              body.commission_pct, body.slippage_pct)
        result["source"] = data["source"]
        result["source_message"] = data["source_message"]
        result["symbol"] = body.symbol
        result["strategy"] = body.strategy
        result["commission_pct"] = body.commission_pct
        result["slippage_pct"] = body.slippage_pct
        result["bar_count"] = data["bar_count"]
        result["bars_requested"] = body.limit
        result["bars_available"] = len(bars)
        if bars:
            result["date_from"] = bars[0].get("date", "")[:10]
            result["date_to"] = bars[-1].get("date", "")[:10]
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
    from api.backtest import run_backtest_script
    from api.data_manager import fetch_bars_waterfall_sync
    def _run():
        hint = (body.broker_hint or "").lower()
        ibkr_cfg = None
        if body.email and (body.use_ibkr or hint == "ibkr"):
            ibkr_cfg = get_ibkr_config(body.email.lower().strip())
        lp_creds = None
        if body.email and hint != "yahoo":
            student = get_student(body.email.lower().strip())
            if student and student.get("app_key"):
                lp_creds = {"app_key": student["app_key"],
                            "app_secret": student["app_secret"],
                            "access_token": student["access_token"]}
        if hint == "yahoo":
            ibkr_cfg = None
            lp_creds = None
        data = fetch_bars_waterfall_sync(
            symbol=body.symbol, timeframe=body.timeframe, limit=body.limit,
            db_path=str(DB_PATH), ibkr_config=ibkr_cfg, lp_credentials=lp_creds,
            skip_cache=body.skip_cache)
        if data["error"]:
            return {"status": "error", "message": data["error"],
                    "source": data["source"], "source_message": data["source_message"]}
        bars = data["bars"]
        if len(bars) < 50:
            return {"status": "error", "message": f"Only {len(bars)} bars, need 50+.",
                    "source": data["source"]}
        _bt_conn = get_db() if body.email else None
        result = run_backtest_script(bars, body.script, body.initial_capital,
                                     params_override=body.params if body.params else None,
                                     email=body.email or None, db_conn=_bt_conn)
        if _bt_conn: _bt_conn.close()
        result["source"] = data["source"]
        result["source_message"] = data["source_message"]
        result["symbol"] = body.symbol
        result["strategy"] = "CUSTOM_SCRIPT"
        result["bar_count"] = data["bar_count"]
        result["bars_requested"] = body.limit
        result["bars_available"] = len(bars)
        if bars:
            result["date_from"] = bars[0].get("date", "")[:10]
            result["date_to"] = bars[-1].get("date", "")[:10]
        return result
    try:
        result = await asyncio.get_event_loop().run_in_executor(_executor, _run)
        return result
    except Exception as e:
        import traceback
        print(f"[RUN-SCRIPT ERROR] {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(400, str(e))


@app.post("/api/backtest/sweep-script")
async def backtest_sweep_script(body: SweepScriptReq):
    from api.backtest import run_backtest_script
    from api.data_manager import fetch_bars_waterfall_sync
    def _run():
        # Get LP credentials if email provided (for LongPort data source)
        lp_creds = None
        if body.email:
            student = get_student(body.email.lower().strip())
            if student and student.get("app_key"):
                lp_creds = {
                    "app_key": student["app_key"],
                    "app_secret": student["app_secret"],
                    "access_token": student["access_token"],
                }
        # Fetch data ONCE for all combos using full waterfall (LP -> Yahoo)
        data = fetch_bars_waterfall_sync(
            symbol=body.symbol, timeframe=body.timeframe, limit=body.limit,
            db_path=str(DB_PATH), lp_credentials=lp_creds)
        if data["error"] or not data["bars"]:
            raise ValueError(f"No data: {data.get('error','No bars returned')}")
        bars = data["bars"]
        source = data["source"]
        results = []
        _sw_conn = get_db() if hasattr(body, 'email') and body.email else None
        for combo in body.combos:
            try:
                r = run_backtest_script(bars, body.script, body.initial_capital,
                                        params_override=combo,
                                        email=getattr(body, 'email', None), db_conn=_sw_conn)
                r["params"] = combo
                r["source"] = source
                results.append(r)
            except Exception as e:
                results.append({"params": combo, "error": str(e)[:200]})
        if _sw_conn: _sw_conn.close()
        # Sort by sharpe (non-error results first)
        results.sort(key=lambda x: x.get("metrics", {}).get("sharpe_ratio", -999), reverse=True)
        # Filter out error-only results for cleaner response
        valid = [r for r in results if "error" not in r]
        return {"results": valid or results, "total": len(body.combos),
                "symbol": body.symbol, "source": source}
    try:
        result = await asyncio.get_event_loop().run_in_executor(_executor, _run)
        return result
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/backtest/optimize-stream")
async def backtest_optimize_stream(request: Request):
    """SSE streaming optimization with walk-forward and Monte Carlo."""
    body = await request.json()
    symbol = body.get("symbol", "")
    timeframe = body.get("timeframe", "1day")
    script = body.get("script", "")
    combos = body.get("combos", [])
    initial_capital = float(body.get("initial_capital", 10000))
    limit = int(body.get("limit", 1260))
    email = body.get("email", "")
    enable_wf = body.get("enable_walk_forward", True)
    enable_mc = body.get("enable_monte_carlo", True)
    wf_window = int(body.get("wf_window", 252))
    wf_step = int(body.get("wf_step", 126))
    wf_min_oos_ratio = float(body.get("wf_min_oos_ratio", 0.0))
    wf_pass_majority = bool(body.get("wf_pass_majority", True))
    commission_pct = float(body.get("commission_pct", 0.0))
    slippage_pct = float(body.get("slippage_pct", 0.0))
    if not script or not combos:
        raise HTTPException(400, "script and combos required")
    # Fetch bars once
    from api.data_manager import fetch_bars_waterfall_sync
    def _get_bars():
        lp_creds = None
        if email:
            student = get_student(email.lower().strip())
            if student and student.get("app_key"):
                lp_creds = {"app_key": student["app_key"],
                            "app_secret": student["app_secret"],
                            "access_token": student["access_token"]}
        return fetch_bars_waterfall_sync(
            symbol=symbol, timeframe=timeframe, limit=limit,
            db_path=str(DB_PATH), lp_credentials=lp_creds)
    bars_result = await asyncio.get_event_loop().run_in_executor(_executor, _get_bars)
    bars = bars_result.get("bars", [])
    if len(bars) < 50:
        raise HTTPException(400, f"Insufficient data: {len(bars)} bars")

    def event_stream():
        from api.backtest import run_optimization_stream
        try:
            gen = run_optimization_stream(
                bars=bars, script=script, combos=combos,
                initial_capital=initial_capital,
                enable_walk_forward=enable_wf, enable_monte_carlo=enable_mc,
                wf_window=wf_window, wf_step=wf_step,
                wf_min_oos_ratio=wf_min_oos_ratio,
                wf_pass_majority=wf_pass_majority,
                commission_pct=commission_pct, slippage_pct=slippage_pct)
            for event in gen:
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/api/options/backtest")
async def options_backtest_stream(request: Request):
    """SSE streaming backtest for options strategies (vertical spreads, condors, etc.)."""
    config = await request.json()

    def event_stream():
        from api.options_backtest import run_options_backtest_stream
        try:
            for event in run_options_backtest_stream(config):
                yield f"data: {json.dumps(event, default=str)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/options/cache/stats")
async def options_cache_stats():
    """Disk-cache inventory: file counts + sizes per symbol."""
    from api.options_data import get_cache_stats
    return get_cache_stats()


_PRECOMPUTE_RESULTS_BUCKET = os.environ.get("R2_RESULTS_BUCKET", "quantx-results")


def _precompute_s3_client():
    """Shared lazily-created R2 client for the precomputed-results routes.
    Uses R2_RESULTS_* env vars when set (quantx-results bucket needs write
    creds), otherwise falls back to the read-path credentials used elsewhere."""
    import boto3 as _boto3
    from api.options_data import R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY
    access = os.environ.get("R2_RESULTS_ACCESS_KEY") or R2_ACCESS_KEY
    secret = os.environ.get("R2_RESULTS_SECRET_KEY") or R2_SECRET_KEY
    return _boto3.client(
        "s3", endpoint_url=R2_ENDPOINT,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="auto",
    )


@app.get("/api/options/precomputed")
async def get_precomputed_result(symbol: str, strategy: str, dte: int, period: str):
    """Return a cached backtest result computed by scripts/precompute_spxw.py.
    404 if the (symbol, strategy, dte, period) combo hasn't been precomputed --
    frontend should fall back to live compute in that case."""
    from fastapi.responses import JSONResponse
    try:
        s3 = _precompute_s3_client()
        key = f"results/{symbol}/{strategy}/{dte}DTE/{period}/default.json"
        obj = s3.get_object(Bucket=_PRECOMPUTE_RESULTS_BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode())
        return JSONResponse(content=data, headers={"X-Result-Source": "precomputed"})
    except Exception:
        raise HTTPException(404, "No pre-computed result available")


@app.get("/api/options/results/index")
async def get_results_index():
    """Master index of precomputed results. Powers the instructor dashboard."""
    try:
        s3 = _precompute_s3_client()
        obj = s3.get_object(Bucket=_PRECOMPUTE_RESULTS_BUCKET, Key="results/index.json")
        return json.loads(obj["Body"].read().decode())
    except Exception:
        raise HTTPException(404, "Results index not found")


# ══════════════════════════════════════════════════════════════════════════════
# OPTIONS STUDIO SHARE LINKS
# ══════════════════════════════════════════════════════════════════════════════
# Saves a backtest result under a short random ID so it can be viewed by
# anyone with the link (read-only). Dual-mode: PostgreSQL when DATABASE_URL is
# set, SQLite otherwise. Both routes are whitelisted from the auth middleware
# so share links remain usable without login.

def _ensure_options_shares_table():
    """Create the options_shares table on first use. Idempotent."""
    if USE_POSTGRES:
        from api.db_postgres import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS options_shares (
                share_id      TEXT PRIMARY KEY,
                email         TEXT,
                config_json   TEXT,
                metrics_json  TEXT,
                trade_log_json TEXT,
                created_at    TEXT
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    else:
        conn = get_db()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS options_shares (
                share_id      TEXT PRIMARY KEY,
                email         TEXT,
                config_json   TEXT,
                metrics_json  TEXT,
                trade_log_json TEXT,
                created_at    TEXT
            )
        """)
        conn.commit()
        conn.close()


@app.post("/api/options/deploy")
async def deploy_options_bot(request: Request):
    """Generate + save a LongPort options bot from an Options Studio config.

    Body: {email, config}
      email  -- student email (used to look up LP creds via get_student)
      config -- Options Studio config dict (same shape as /api/options/backtest)
    """
    body = await request.json()
    email = (body.get("email") or "").strip().lower()
    config = body.get("config") or {}

    if not email:
        raise HTTPException(400, "email required")
    if not config.get("strategy_type"):
        raise HTTPException(400, "strategy_type required")
    if not config.get("symbol"):
        raise HTTPException(400, "symbol required")

    # Pull credentials from the legacy single-set-per-student store
    # (matches the pattern used by all other LP routes in this file).
    student = get_student(email)
    if not student or not student.get("app_key"):
        raise HTTPException(400, "No LongPort credentials on file for this student")

    student_payload = {
        "email":           email,
        "app_key":         student["app_key"],
        "app_secret":      student["app_secret"],
        "access_token":    student["access_token"],
        "central_api_url": CENTRAL_API_URL,
    }

    # Generate + save the bot script under bots/<email>/bot_<symbol>_<strat>_<dte>DTE.py
    from api.generate import save_lp_options_bot
    from api.config import BOTS_DIR as _BOTS_DIR
    output_dir = _BOTS_DIR / email
    try:
        script_path = save_lp_options_bot(
            config=config, student=student_payload,
            output_path=str(output_dir),
        )
    except Exception as e:
        _log.exception("deploy: generate failed")
        raise HTTPException(500, f"Bot generation failed: {e}")

    # Register the strategy in the DB (positional args, matches save_strategy signature)
    strategy_id = f"opts_{config['symbol']}_{config['strategy_type']}_{int(time.time())}"
    strategy_name = f"{config['symbol']} {config['strategy_type']} {config.get('target_dte', 7)}DTE"
    try:
        save_strategy(
            email,                    # email
            strategy_id,              # strategy_id
            strategy_name,            # strategy_name
            config["symbol"],         # symbol
            "options",                # arena
            "",                       # timeframe (unused for options)
            config,                   # conditions (stored as JSON)
            {},                       # exit_rules (options engine owns its exits)
            {},                       # risk (options engine owns its risk)
            is_active=False,          # don't auto-start -- student reviews DRY_RUN first
            mode="options",
            library_id="",
            custom_script=script_path,
            broker="longport",
        )
    except Exception as e:
        _log.exception("deploy: save_strategy failed")
        # The script is already on disk; don't wipe it. Surface the error.
        raise HTTPException(500, f"Strategy save failed: {e}")

    return {
        "ok": True,
        "strategy_id": strategy_id,
        "script_path": script_path,
        "message": f"Options bot generated for {config['symbol']} {config['strategy_type']} "
                   f"(DRY_RUN=True; review the script before enabling live trading)",
    }


@app.post("/api/options/share")
async def options_share_create(request: Request):
    """Save a backtest result under a short random ID. Returns {share_id, url}."""
    body = await request.json()
    _ensure_options_shares_table()

    share_id = secrets.token_urlsafe(8)
    config = json.dumps(body.get("config") or {}, default=str)
    metrics = json.dumps(body.get("metrics") or {}, default=str)
    # Defensive: cap trade log to 100 even if frontend forgot to slice
    trades = (body.get("trade_log") or [])[:100]
    trade_log = json.dumps(trades, default=str)
    email = (body.get("email") or "").strip()
    created = datetime.utcnow().isoformat()

    if USE_POSTGRES:
        from api.db_postgres import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO options_shares
              (share_id, email, config_json, metrics_json, trade_log_json, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (share_id, email, config, metrics, trade_log, created))
        conn.commit()
        cur.close()
        conn.close()
    else:
        conn = get_db()
        conn.execute("""
            INSERT INTO options_shares
              (share_id, email, config_json, metrics_json, trade_log_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (share_id, email, config, metrics, trade_log, created))
        conn.commit()
        conn.close()

    return {"share_id": share_id, "url": "/#options-share/" + share_id}


@app.get("/api/options/share/{share_id}")
async def options_share_get(share_id: str):
    """Retrieve a shared backtest. Public -- no auth required."""
    _ensure_options_shares_table()
    if USE_POSTGRES:
        from api.db_postgres import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT share_id, config_json, metrics_json, trade_log_json, created_at "
            "FROM options_shares WHERE share_id = %s", (share_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
    else:
        conn = get_db()
        row = conn.execute(
            "SELECT share_id, config_json, metrics_json, trade_log_json, created_at "
            "FROM options_shares WHERE share_id = ?", (share_id,)).fetchone()
        conn.close()
    if not row:
        raise HTTPException(404, "Share not found")
    return {
        "share_id": row["share_id"],
        "config": json.loads(row["config_json"] or "{}"),
        "metrics": json.loads(row["metrics_json"] or "{}"),
        "trade_log": json.loads(row["trade_log_json"] or "[]"),
        "created_at": row["created_at"],
    }


@app.get("/api/options/dates/{symbol}")
async def get_options_dates(symbol: str):
    """Return the actual date range available in R2 for a symbol.
    Bypasses the TTL cache to ensure frontend sees fresh bucket contents."""
    from api.options_data import get_available_dates, invalidate_dates_cache
    invalidate_dates_cache(symbol)
    dates = get_available_dates(symbol)
    return {
        "symbol": symbol.upper(),
        "dates": dates,
        "count": len(dates),
        "first": dates[0] if dates else None,
        "last": dates[-1] if dates else None,
    }


@app.delete("/api/options/cache")
async def options_cache_clear(symbol: Optional[str] = Query(None)):
    """Delete cache files. Pass ?symbol=SPY to clear one symbol, omit to clear all."""
    from api.options_data import clear_cache
    return clear_cache(symbol)


@app.post("/api/options/cache/preload")
async def options_cache_preload(request: Request):
    """SSE stream: pre-download all R2 parquet files for [start_date, end_date]."""
    body = await request.json()
    symbol = body["symbol"].upper()
    start = body["start_date"]
    end = body["end_date"]

    def event_stream():
        from api.options_data import get_available_dates, _ensure_day_cached, get_cache_stats
        try:
            dates = [d for d in get_available_dates(symbol) if start <= d <= end]
            total = len(dates)
            yield f"data: {json.dumps({'type':'start','total':total,'symbol':symbol})}\n\n"
            for i, d in enumerate(dates, 1):
                _ensure_day_cached(symbol, d)
                yield f"data: {json.dumps({'type':'progress','done':i,'total':total,'date':d})}\n\n"
            stats = get_cache_stats()
            sym_entry = next((s for s in stats["symbols"] if s["symbol"] == symbol), None)
            yield f"data: {json.dumps({'type':'complete','cached_files':sym_entry['files'] if sym_entry else total,'size_mb':sym_entry['size_mb'] if sym_entry else 0})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/api/options/cache/prewarm-popular")
async def prewarm_popular(request: Request, background_tasks: BackgroundTasks):
    """Fire-and-forget pre-warm of the most commonly backtested symbols.

    Gated by an admin PIN (the `key` body field). The actual downloads
    run in a FastAPI BackgroundTasks worker so the HTTP response returns
    immediately; watch Railway logs for `[prewarm]` lines.
    """
    body = await request.json()
    expected_pin = os.environ.get("ADMIN_PIN", "quantx2025")
    if body.get("key") != expected_pin:
        raise HTTPException(403, "Unauthorized")

    symbols = body.get("symbols") or ["SPY", "SPXW"]
    from datetime import date as _date
    end = _date.today()
    start = _date(end.year - 2, end.month, end.day)
    start_str, end_str = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    def _prewarm_all():
        # Stream R2 -> disk directly. NO pandas load, NO parallel workers --
        # the previous version OOM'd Railway because 4 ThreadPool workers each
        # held a ~600 MB DataFrame at once (4 * 600 = 2.4 GB peak).
        # This version peaks at the size of one parquet file (~215 MB raw).
        # Trade-off: stores the FULL raw R2 file (~215 MB), not the slim 50 MB
        # cleaned version that _ensure_day_cached produces. Backtests that
        # later read these files will need ~600 MB DataFrame memory at read
        # time. Acceptable for prewarm-and-go: the OOM happened at prewarm,
        # not at backtest, and backtests serialize one file at a time.
        import boto3 as _boto3
        import botocore.config as _botoconf
        import time as _time
        import pyarrow.parquet as _pq
        from api.options_data import (
            CACHE_DIR, get_available_dates,
            R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET,
        )

        s3 = _boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name="auto",
            config=_botoconf.Config(read_timeout=300, connect_timeout=30),
        )

        for symbol in symbols:
            sym_up = symbol.upper()
            try:
                all_dates = get_available_dates(sym_up)
                dates = [d for d in all_dates if start_str <= d <= end_str]
                sym_dir = CACHE_DIR / sym_up
                sym_dir.mkdir(parents=True, exist_ok=True)
                print(f"[prewarm] {sym_up}: {len(dates)} dates to download")
                done = 0
                skipped = 0
                for d in dates:
                    cf = sym_dir / f"{d}.parquet"
                    # Skip if already cached and valid
                    if cf.exists():
                        try:
                            _pq.read_schema(cf)
                            skipped += 1
                            continue
                        except Exception:
                            cf.unlink(missing_ok=True)
                    # Download directly to disk -- key matches the R2 layout
                    # used by api/options_data._download_day, NOT a flat
                    # SYMBOL/DATE.parquet path.
                    try:
                        key = f"{sym_up}/greeks_1min_daily/{d}.parquet"
                        s3.download_file(R2_BUCKET, key, str(cf))
                        done += 1
                        if done % 10 == 0:
                            print(f"[prewarm] {sym_up}: {done} downloaded, "
                                  f"{skipped} already cached")
                        _time.sleep(0.05)  # be nice to R2
                    except Exception as e:
                        print(f"[prewarm] SKIP {sym_up} {d}: {e}")
                        cf.unlink(missing_ok=True)
                print(f"[prewarm] {sym_up}: DONE -- "
                      f"{done} downloaded, {skipped} already cached")
            except Exception as e:
                print(f"[prewarm] {sym_up} ERROR: {e}")

    background_tasks.add_task(_prewarm_all)
    return {
        "ok": True,
        "message": f"Pre-warming {symbols} in background",
        "symbols": symbols,
        "date_range": {"start": start_str, "end": end_str},
        "note": "Check Railway logs for [prewarm] lines",
    }


@app.post("/api/options/cache/repair")
async def repair_cache(request: Request, background_tasks: BackgroundTasks):
    """Scan cache and delete corrupted parquet files. Admin-gated. Fire-and-forget
    background scan. Railway logs will show [repair] lines per bad file."""
    body = await request.json()
    if body.get("key") != os.environ.get("ADMIN_PIN", "quantx2025"):
        raise HTTPException(403, "Unauthorized")

    def _repair():
        import pyarrow.parquet as pq
        from api.options_data import CACHE_DIR
        deleted = 0
        checked = 0
        if not CACHE_DIR.exists():
            print("[repair] Cache dir does not exist; nothing to do")
            return
        for sym_dir in CACHE_DIR.iterdir():
            if not sym_dir.is_dir():
                continue
            for f in sym_dir.glob("*.parquet"):
                checked += 1
                try:
                    pq.read_schema(f)
                except Exception as e:
                    print(f"[repair] Corrupted: {f.name} -- {type(e).__name__}: {e}")
                    try:
                        f.unlink(missing_ok=True)
                        deleted += 1
                    except Exception as e2:
                        print(f"[repair] Unlink failed: {f.name}: {e2}")
        print(f"[repair] Done: checked={checked} deleted={deleted}")

    background_tasks.add_task(_repair)
    return {"ok": True, "message": "Repair scan running in background",
            "note": "Check Railway logs for [repair] lines"}


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
        ctx = get_lp_quote_ctx(student["app_key"], student["app_secret"], student["access_token"])
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


@app.get("/api/indicators-registry")
async def get_indicators_registry():
    """In-memory indicator registry (builder UI). For DB-backed list, use GET /api/indicators."""
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
        # Frontend uses these to render the status bar + cache messaging.
        "backtest_source": "longport",
        "trading_mode": "cloud" if HOSTING == "railway" else "local",
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
    from api.config import STATE_DIR
    email = email.lower().strip()
    strats = get_strategies(email)
    for s in strats:
        sid = s.get("strategy_id", "")
        state_file = STATE_DIR / f"pos_{sid}.json"
        s["current_position"] = 0
        s["current_side"] = "flat"
        s["position_entry_price"] = 0.0
        if state_file.exists():
            try:
                st = json.loads(state_file.read_text(encoding="utf-8"))
                s["current_position"] = int(st.get("current_position", 0))
                s["current_side"] = "long" if s["current_position"] > 0 else ("short" if s["current_position"] < 0 else "flat")
                s["position_entry_price"] = float(st.get("entry_price", 0))
            except Exception:
                pass
    return {"strategies": strats}


def _cancel_student_orders(student: dict) -> int:
    """Cancel all open LongPort orders for a student. Returns count cancelled."""
    logger = _logging.getLogger("quantx-deployer")
    try:
        from longport.openapi import OrderStatus  # noqa: F401  (used below via status name)
        ctx = get_lp_trade_ctx(student["app_key"], student["app_secret"], student["access_token"])
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


def read_open_positions(email: str) -> list:
    """Read all state JSON files for a student and return open positions."""
    from api.config import STATE_DIR
    open_positions = []
    try:
        strategies = get_strategies(email, active_only=False)
        for s in strategies:
            sid = s.get("strategy_id", "")
            state_file = STATE_DIR / f"pos_{sid}.json"
            if state_file.exists():
                try:
                    state = json.loads(state_file.read_text(encoding="utf-8"))
                    pos = int(state.get("current_position", state.get("position", 0)))
                    if pos != 0:
                        open_positions.append({
                            "strategy_id": sid,
                            "symbol": s.get("symbol", ""),
                            "position": pos,
                            "entry_price": float(state.get("entry_price", 0)),
                            "side": "long" if pos > 0 else "short",
                            "state_file": str(state_file),
                        })
                except Exception as e:
                    _log.warning("Could not read state for %s: %s", sid, e)
    except Exception as e:
        _log.warning("read_open_positions failed: %s", e)
    return open_positions


def _close_lp_positions(student: dict, positions: list) -> dict:
    """Market-sell all open positions via LongPort. Returns {closed, failed}."""
    closed, failed = [], []
    if not positions:
        return {"closed": closed, "failed": failed}
    try:
        from longport.openapi import OrderType, OrderSide, TimeInForceType
        import decimal
        trade_ctx = get_lp_trade_ctx(student["app_key"], student["app_secret"], student["access_token"])
        for pos in positions:
            symbol = pos["symbol"]
            qty = abs(int(pos["position"]))
            side = pos.get("side", "long")
            order_side = OrderSide.Sell if side == "long" else OrderSide.Buy
            try:
                resp = trade_ctx.submit_order(
                    symbol=symbol, order_type=OrderType.MO, side=order_side,
                    submitted_quantity=decimal.Decimal(qty),
                    time_in_force=TimeInForceType.Day,
                    remark="QuantX safe redeploy -- closing position")
                _log.info("[REDEPLOY] Closed %s x%d %s | order_id=%s",
                         symbol, qty, side, resp.order_id if resp else "?")
                closed.append({**pos, "order_id": str(resp.order_id) if resp else ""})
                sf = pos.get("state_file")
                if sf and Path(sf).exists():
                    sd = json.loads(Path(sf).read_text())
                    sd["current_position"] = 0
                    sd["entry_price"] = 0.0
                    Path(sf).write_text(json.dumps(sd, indent=2))
            except Exception as e:
                _log.error("[REDEPLOY] Failed to close %s: %s", symbol, e)
                failed.append({**pos, "error": str(e)})
        time.sleep(2)
    except Exception as e:
        _log.error("[REDEPLOY] TradeContext failed: %s", e)
        failed = positions
    return {"closed": closed, "failed": failed}


@app.get("/api/deploy/check")
async def deploy_check(email: str = Query("")):
    """Check for open positions before deploying."""
    if not email:
        raise HTTPException(400, "email required")
    email = email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not registered")
    open_positions = read_open_positions(email)
    is_running = email in _running_processes and \
                 _running_processes[email].poll() is None
    return {
        "is_running": is_running,
        "open_positions": open_positions,
        "has_open_positions": len(open_positions) > 0,
        "position_count": len(open_positions),
        "symbols": [p["symbol"] for p in open_positions],
    }


@app.post("/api/deploy/close-and-redeploy")
async def close_and_redeploy(req: DeployReq):
    """Close all open positions then redeploy."""
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not registered")
    open_positions = read_open_positions(email)
    close_result = {"closed": [], "failed": []}
    if open_positions:
        _log.info("[REDEPLOY] Closing %d positions before redeploy", len(open_positions))
        close_result = _close_lp_positions(student, open_positions)
    deploy_result = await deploy(req)
    if isinstance(deploy_result, dict):
        deploy_result["positions_closed"] = close_result["closed"]
        deploy_result["positions_failed"] = close_result["failed"]
    return deploy_result


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

    # Split strategies by broker and type
    lp_strats = [s for s in strategies if s.get("broker", "longport") != "ibkr"]
    ibkr_strats = [s for s in strategies if s.get("broker") == "ibkr"]

    # Check for simple/quick_test strategies — deploy as individual simple bots
    deployed_simple = []
    for s in strategies:
        conds = s.get("conditions", {})
        is_quick = (s.get("library_id") == "QUICK_TEST" or
                    conds.get("type") == "quick_test" or
                    s.get("mode") == "quick_test")
        if is_quick:
            sym = s.get("symbol", "700.HK")
            broker = s.get("broker", "longport")
            try:
                if broker == "ibkr":
                    ibkr_cfg = get_ibkr_config(email) or {"host": "127.0.0.1", "port": 7497, "client_id": 1}
                    sp, lp = generate_simple_ibkr_bot(email, sym, ibkr_cfg, student)
                else:
                    sp, lp = generate_simple_lp_bot(email, sym, student)
                proc = _launch_bot(sp, lp)
                save_process(email, proc.pid, "running", sp, lp)
                deployed_simple.append({"strategy_id": s["strategy_id"], "pid": proc.pid, "broker": broker})
                _log.info("[DEPLOY] Simple %s bot PID: %s for %s", broker, proc.pid, sym)
            except Exception as e:
                import traceback
                _log.error("[DEPLOY] Simple bot FAILED for %s: %s\n%s", sym, e, traceback.format_exc())
            # Remove from regular deploy lists
            lp_strats = [x for x in lp_strats if x["strategy_id"] != s["strategy_id"]]
            ibkr_strats = [x for x in ibkr_strats if x["strategy_id"] != s["strategy_id"]]

    # Deploy options bots (sec_type=OPT)
    for s in list(ibkr_strats):
        conds = s.get("conditions", {})
        if conds.get("sec_type") == "OPT" or s.get("mode") == "options":
            try:
                from api.generate import generate_options_bot
                ibkr_cfg = get_ibkr_config(email) or {"host": "127.0.0.1", "port": 7497, "client_id": 1}
                ibkr_cfg["central_api_url"] = central_url
                ibkr_cfg["account_id"] = ibkr_cfg.get("account_id", "")
                ibkr_cfg["port"] = conds.get("port", ibkr_cfg.get("port", 7497))
                sp, lp, tp = generate_options_bot(email, conds, ibkr_cfg)
                proc = _launch_bot(sp, lp)
                save_process(email, proc.pid, "running", sp, lp)
                _log.info("[DEPLOY] Options bot PID: %s for %s", proc.pid, s["strategy_id"])
            except Exception as e:
                import traceback
                _log.error("[DEPLOY] Options bot FAILED: %s\n%s", e, traceback.format_exc())
            ibkr_strats = [x for x in ibkr_strats if x["strategy_id"] != s["strategy_id"]]

    # If there are IBKR strategies, deploy each via production bot generator
    if ibkr_strats:
        ibkr_cfg = get_ibkr_config(email) or {"host": "127.0.0.1", "port": 7497, "client_id": 1}
        ibkr_cfg["central_api_url"] = central_url
        ibkr_cfg["account_id"] = ibkr_cfg.get("account_id", "")
        for s in ibkr_strats:
            try:
                # Build strategy config for generate_ibkr_bot_prod
                conds = s.get("conditions", {})
                # If library strategy, convert to Builder conditions
                if not conds.get("entry_long") and s.get("library_id"):
                    conds = library_id_to_conditions(s["library_id"])
                risk = s.get("risk", {})
                strat_config = {
                    "strategy_id": s["strategy_id"],
                    "symbol": s["symbol"],
                    "sec_type": conds.get("sec_type", "STK"),
                    "exchange": conds.get("exchange", "SMART"),
                    "currency": conds.get("currency", "USD"),
                    "lot_size": int(risk.get("lots", 1)),
                    "max_capital": float(s.get("allocation", 1000)),
                    "bar_size": conds.get("bar_size", "1 min"),
                    "interval_minutes": int(conds.get("interval_minutes", 1)),
                    "stop_loss_pct": float(risk.get("sl_pct", 2)) / 100 if float(risk.get("sl_pct", 2)) > 1 else float(risk.get("sl_pct", 0.02)),
                    "take_profit_pct": float(risk.get("tp_pct", 5)) / 100 if float(risk.get("tp_pct", 5)) > 1 else float(risk.get("tp_pct", 0.05)),
                    "has_short": bool(conds.get("entry_short")),
                    "entry_long": conds.get("entry_long", []),
                    "exit_long": conds.get("exit_long", []),
                    "entry_short": conds.get("entry_short", []),
                    "exit_short": conds.get("exit_short", []),
                    "entry_long_logic": conds.get("entry_long_logic", "AND"),
                    "exit_long_logic": conds.get("exit_long_logic", "OR"),
                }
                sp, lp, tp = generate_ibkr_bot_prod(email, strat_config, ibkr_cfg)
                proc = _launch_bot(sp, lp)
                save_process(email, proc.pid, "running", sp, lp)
                _log.info("[DEPLOY] IBKR prod bot PID: %s for %s %s", proc.pid, s["strategy_id"], s["symbol"])
            except Exception as e:
                import traceback
                _log.error("[DEPLOY] IBKR prod bot FAILED for %s: %s\n%s", s["strategy_id"], e, traceback.format_exc())

    # If no LP strategies remain, we're done (all IBKR)
    if not lp_strats and ibkr_strats:
        return {
            "status": "deployed",
            "broker": "ibkr",
            "strategies_count": len(ibkr_strats),
            "central_api_url": central_url,
        }

    if not lp_strats:
        return {"status": "deployed", "strategies_count": 0, "central_api_url": central_url}

    # Generate LP master bot (shared connections for ALL LP strategies)
    # get_student() already returns decrypted credentials
    lp_creds = {
        "app_key": student.get("app_key", ""),
        "app_secret": student.get("app_secret", ""),
        "access_token": student.get("access_token", ""),
        "central_api_url": central_url,
    }

    # Read existing states for position preservation across redeploy
    existing_states = {}
    for pos in read_open_positions(email):
        existing_states[pos["strategy_id"]] = pos

    try:
        script_path, log_path = generate_lp_master_bot(
            email, lp_strats, lp_creds, initial_states=existing_states)
    except Exception as e:
        import traceback
        _log.error("[DEPLOY] LP master gen failed: %s\n%s", e, traceback.format_exc())
        # Fallback to old master bot
        script_path = generate_master_bot(email, lp_strats, student)
        email_safe = email.replace("@", "_at_").replace(".", "_")
        logs_dir = Path(script_path).parent.parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_path = str(logs_dir / f"{email_safe}_master.log")

    _log.info("[DEPLOY] Running LP master: %s | Log: %s", script_path, log_path)

    try:
        proc = _launch_bot(script_path, log_path)
        _running_processes[email] = proc
        save_process(email, proc.pid, "running", script_path, log_path)
        _log.info("[DEPLOY] LP master PID: %s (%d strategies, 2 connections)", proc.pid, len(lp_strats))

        # Wait 3 seconds to check for immediate crash
        time.sleep(3)
        exit_code = proc.poll()
        if exit_code is not None:
            del _running_processes[email]
            update_process_status(email, "error", f"Crashed on startup. Exit code: {exit_code}")
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
            "lp_strategies": len(lp_strats),
            "ibkr_strategies": len(ibkr_strats),
            "lp_connections": 2,
            "central_api_url": central_url,
            "old_orders_cancelled": old_cancelled,
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to launch bot: {e}")


@app.post("/api/stop")
async def stop(req: StopReq):
    email = req.email.lower().strip()
    broker = req.broker or "all"
    stopped = []
    # Stop by broker type using psutil
    if broker in ("all", "longport"):
        if email in _running_processes:
            _stop_process(email)
            stopped.append("longport")
        else:
            # Try killing from DB PID
            _kill_process_by_db(email, ibkr=False)
            stopped.append("longport")
    if broker in ("all", "ibkr"):
        _kill_process_by_db(email, ibkr=True)
        stopped.append("ibkr")
    if not stopped and email not in _running_processes:
        return {"status": "stopped", "message": "No running bot found"}
    return {"status": "stopped", "brokers_stopped": stopped}


def _kill_process_by_db(email: str, ibkr: bool = False):
    """Kill a process found in the DB by its PID."""
    conn = get_db()
    try:
        pattern = '%ibkr%' if ibkr else '%master%'
        not_pattern = '%ibkr%'
        if ibkr:
            row = conn.execute("SELECT pid FROM processes WHERE email=? AND master_script_path LIKE '%ibkr%' AND status='running' ORDER BY id DESC LIMIT 1", (email,)).fetchone()
        else:
            row = conn.execute("SELECT pid FROM processes WHERE email=? AND master_script_path NOT LIKE '%ibkr%' AND status='running' ORDER BY id DESC LIMIT 1", (email,)).fetchone()
    finally:
        conn.close()
    if not row:
        return
    try:
        import psutil
        p = psutil.Process(row["pid"])
        p.terminate()
        p.wait(timeout=10)
    except Exception:
        try:
            import psutil
            psutil.Process(row["pid"]).kill()
        except Exception:
            pass
    update_process_status(email, "stopped")


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


@app.post("/api/test-ibkr-connection")
async def test_ibkr_connection(req: IBKRConfigReq):
    def _test_sync():
        import asyncio as _asyncio
        # Create event loop for this thread BEFORE importing ib_insync
        _loop = _asyncio.new_event_loop()
        _asyncio.set_event_loop(_loop)
        try:
            from ib_insync import IB
            ib = IB()
            ib.connect(req.host, req.port, clientId=req.client_id + 50, timeout=10)
            connected = ib.isConnected()
            accounts = list(ib.managedAccounts()) if connected else []
            ib.disconnect()
            if connected:
                return {"ok": True, "message": "Connected to IBKR",
                        "account": accounts[0] if accounts else "unknown"}
            return {"ok": False, "message": "Connection returned but isConnected=False"}
        except Exception as e:
            return {"ok": False, "message": str(e)}
        finally:
            _loop.close()
    try:
        result = await asyncio.get_event_loop().run_in_executor(_executor, _test_sync)
        return result
    except Exception as e:
        return {"ok": False, "message": f"Server error: {e}"}


# ── Broker accounts ──────────────────────────────────────────────────────────

@app.get("/api/broker-accounts")
async def list_broker_accounts(email: str = Query("")):
    if not email:
        return {"accounts": []}
    accounts = get_broker_accounts(email.lower().strip())
    return {"accounts": accounts}


@app.post("/api/broker-accounts")
async def add_broker_account(request: Request):
    body = await request.json()
    email = (body.get("email", "") or "").lower().strip()
    broker = body.get("broker", "")
    acct_type = body.get("account_type", "paper")
    if not email or not broker:
        raise HTTPException(400, "email and broker required")
    aid = save_broker_account(
        email=email, broker=broker, account_type=acct_type,
        nickname=body.get("nickname", f"{broker.upper()} {acct_type.title()}"),
        account_id=body.get("account_id", ""),
        app_key=body.get("app_key", ""),
        app_secret=body.get("app_secret", ""),
        access_token=body.get("access_token", ""),
        ibkr_host=body.get("ibkr_host", "127.0.0.1"),
        ibkr_port=int(body.get("ibkr_port", 7497)),
    )
    # Also save to legacy tables for backward compat
    if broker == "longport" and body.get("app_key"):
        student = get_student(email)
        if student:
            save_student(email, student.get("name", ""), body["app_key"],
                        body["app_secret"], body["access_token"],
                        student.get("central_api_url", ""))
    elif broker == "ibkr":
        save_ibkr_config(email, body.get("ibkr_host", "127.0.0.1"),
                        int(body.get("ibkr_port", 7497)), 1)
    return {"status": "saved", "id": aid}


@app.delete("/api/broker-accounts/{account_id}")
async def remove_broker_account(account_id: int):
    ok = delete_broker_account(account_id)
    if not ok:
        raise HTTPException(404, "Account not found")
    return {"status": "deleted"}


@app.post("/api/broker-accounts/{account_id}/test")
async def test_broker_account(account_id: int):
    acct = get_broker_credentials(account_id)
    if not acct:
        raise HTTPException(404, "Account not found")
    broker = acct.get("broker", "")
    def _test():
        if broker == "longport":
            try:
                ctx = get_lp_quote_ctx(acct["app_key"], acct["app_secret"], acct["access_token"])
                quotes = ctx.quote(["700.HK"])
                if quotes:
                    price = float(quotes[0].last_done)
                    update_broker_account_status(account_id, True)
                    return {"ok": True, "message": f"Connected -- 700.HK: ${price:.2f}"}
                update_broker_account_status(account_id, True)
                return {"ok": True, "message": "Connected (no quote data)"}
            except Exception as e:
                update_broker_account_status(account_id, False, str(e))
                return {"ok": False, "message": str(e)}
        elif broker == "ibkr":
            try:
                import asyncio as _aio
                _loop = _aio.new_event_loop()
                _aio.set_event_loop(_loop)
                try:
                    from ib_insync import IB
                    ib = IB()
                    ib.connect(acct.get("ibkr_host", "127.0.0.1"),
                              int(acct.get("ibkr_port", 7497)),
                              clientId=int(acct.get("id", 1)) + 50, timeout=10)
                    connected = ib.isConnected()
                    accounts = list(ib.managedAccounts()) if connected else []
                    ib.disconnect()
                    if connected:
                        aid_str = accounts[0] if accounts else ""
                        if aid_str:
                            # Update account_id in DB
                            conn = get_db()
                            conn.execute("UPDATE broker_accounts SET account_id=? WHERE id=?",
                                        (aid_str, account_id))
                            conn.commit(); conn.close()
                        update_broker_account_status(account_id, True)
                        return {"ok": True, "message": f"Connected to IBKR",
                                "account": aid_str}
                    update_broker_account_status(account_id, False, "isConnected=False")
                    return {"ok": False, "message": "Connection returned but isConnected=False"}
                except Exception as e:
                    update_broker_account_status(account_id, False, str(e))
                    return {"ok": False, "message": str(e)}
                finally:
                    _loop.close()
            except Exception as e:
                return {"ok": False, "message": str(e)}
        return {"ok": False, "message": f"Unknown broker: {broker}"}
    try:
        result = await asyncio.get_event_loop().run_in_executor(_executor, _test)
        return result
    except Exception as e:
        return {"ok": False, "message": f"Test error: {e}"}


# ── Data cache endpoints ─────────────────────────────────────────────────────

@app.get("/api/data-cache")
async def data_cache_list(email: str = Query("")):
    from api.data_manager import get_cached_symbols
    cached = get_cached_symbols(str(DB_PATH))
    return {"cached": cached, "count": len(cached)}


@app.delete("/api/data-cache")
async def data_cache_clear(symbol: str = Query(""), timeframe: str = Query("")):
    from api.data_manager import clear_cached_symbol
    if symbol:
        clear_cached_symbol(str(DB_PATH), symbol.upper(), timeframe or None)
        return {"ok": True, "cleared": symbol}
    return {"ok": False, "message": "symbol required"}


@app.post("/api/data-prefetch")
async def data_prefetch(request: Request):
    body = await request.json()
    symbol = (body.get("symbol", "") or "").upper().strip()
    timeframe = body.get("timeframe", "1day")
    limit = int(body.get("limit", 252))
    email = body.get("email", "")
    if not symbol:
        return {"ok": False, "message": "symbol required"}
    def _fetch():
        from api.data_manager import fetch_bars_waterfall_sync
        ibkr_cfg = None
        if email:
            ibkr_cfg = get_ibkr_config(email.lower().strip())
        result = fetch_bars_waterfall_sync(symbol=symbol, timeframe=timeframe, limit=limit,
                                           db_path=str(DB_PATH), ibkr_config=ibkr_cfg)
        _log.info("Prefetch %s/%s: %s (%d bars)", symbol, timeframe, result["source"], result["bar_count"])
    asyncio.get_event_loop().run_in_executor(_executor, _fetch)
    return {"ok": True, "message": f"Fetching {symbol}/{timeframe} in background..."}


# ── Indicators library ────────────────────────────────────────────────────

@app.get("/api/indicators")
async def list_indicators(category: str = Query(""), created_by: str = Query("")):
    conn = get_db()
    try:
        q = "SELECT * FROM indicators WHERE 1=1"
        params = []
        if category:
            q += " AND category=?"; params.append(category)
        if created_by:
            q += " AND created_by=?"; params.append(created_by)
        q += " ORDER BY is_builtin DESC, category, name"
        rows = conn.execute(q, params).fetchall()
        indicators = []
        cats = {}
        for r in rows:
            d = dict(r)
            d["output_labels"] = json.loads(d.get("output_labels") or "[]")
            d["params"] = json.loads(d.get("params") or "[]")
            indicators.append(d)
            cat = d.get("category", "custom")
            cats[cat] = cats.get(cat, 0) + 1
        custom_ct = sum(1 for i in indicators if not i.get("is_builtin"))
        return {"indicators": indicators, "categories": cats, "total": len(indicators), "custom_count": custom_ct}
    finally:
        conn.close()


@app.get("/api/indicators/custom")
async def indicators_custom(email: str = Query("", description="Unused, kept for frontend compat")):
    """List custom (non-builtin) indicators. Must be before /{indicator_id} route."""
    from api.database import get_custom_indicators
    conn = get_db()
    try:
        indicators = get_custom_indicators(conn)
        return {"indicators": indicators, "count": len(indicators)}
    finally:
        conn.close()


@app.get("/api/indicators/{indicator_id}")
async def get_indicator(indicator_id: str):
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM indicators WHERE indicator_id=?", (indicator_id.upper(),)).fetchone()
        if not row:
            raise HTTPException(404, "Indicator not found")
        d = dict(row)
        d["output_labels"] = json.loads(d.get("output_labels") or "[]")
        d["params"] = json.loads(d.get("params") or "[]")
        return d
    finally:
        conn.close()


@app.post("/api/indicators")
async def register_indicator(request: Request):
    body = await request.json()
    validate_only = request.query_params.get("validate_only", "false") == "true"
    errors = []
    # 1. Required fields
    for f in ["id", "name", "calc_code"]:
        if not body.get(f):
            errors.append(f"Missing required field: {f}")
    if errors:
        return {"status": "error", "errors": errors}
    ind_id = body["id"].upper().replace(" ", "_")
    # 2. Format check
    import re
    if not re.match(r'^[A-Z][A-Z0-9_]{1,29}$', ind_id):
        errors.append("ID must be UPPER_SNAKE_CASE, 2-30 chars, start with letter")
    # 3. Duplicate check
    conn = get_db()
    existing = conn.execute("SELECT 1 FROM indicators WHERE indicator_id=?", (ind_id,)).fetchone()
    if existing and not validate_only:
        errors.append(f"Indicator {ind_id} already exists")
    # 4. Code safety
    calc_code = body.get("calc_code", "")
    import ast
    try:
        tree = ast.parse(calc_code)
    except SyntaxError as e:
        errors.append(f"Invalid Python syntax: {e}")
        tree = None
    if tree:
        import re as _re
        _forbidden_patterns = {
            "import": r'\bimport\b', "exec": r'\bexec\s*\(', "eval": r'\beval\s*\(',
            "open": r'\bopen\s*\(', "__import__": r'__import__', "subprocess": r'\bsubprocess\b',
            "os.": r'\bos\.', "sys.": r'\bsys\.', "socket": r'\bsocket\b',
            "requests": r'\brequests\b', "shutil": r'\bshutil\b',
        }
        for label, pattern in _forbidden_patterns.items():
            if _re.search(pattern, calc_code):
                errors.append(f"Forbidden keyword: {label}")
        if calc_code.count("\n") > 60:
            errors.append("Code too long (max 60 lines)")
    # 5. Test execution
    if not errors and tree:
        try:
            import math
            ns = {"math": math, "__builtins__": {
                "len": len, "range": range, "list": list, "dict": dict, "tuple": tuple,
                "set": set, "min": min, "max": max, "abs": abs, "sum": sum, "round": round,
                "zip": zip, "enumerate": enumerate, "print": print, "int": int,
                "float": float, "True": True, "False": False, "None": None, "bool": bool,
                "str": str, "sorted": sorted, "reversed": reversed, "map": map,
                "filter": filter, "isinstance": isinstance, "any": any, "all": all,
                "type": type, "hasattr": hasattr, "getattr": getattr,
            }}
            exec(calc_code, ns)
            fn_name = f"calc_{ind_id.lower()}"
            if fn_name not in ns:
                errors.append(f"Function {fn_name} not found in calc_code")
            else:
                import inspect
                dummy = [100.0 + i * 0.1 for i in range(100)]
                fn = ns[fn_name]
                sig = inspect.signature(fn)
                positional = [p for p in sig.parameters.values()
                              if p.default is inspect.Parameter.empty]
                call_args = [dummy] * min(len(positional), 5)
                result = fn(*call_args)
                if isinstance(result, tuple) and all(isinstance(r, list) for r in result):
                    for idx, r in enumerate(result):
                        if len(r) != 100:
                            errors.append(f"Output {idx} length {len(r)} != input length 100")
                elif isinstance(result, list):
                    if len(result) != 100:
                        errors.append(f"Output length {len(result)} != input length 100")
                else:
                    errors.append("calc_code must return a list or tuple of lists")
        except Exception as e:
            errors.append(f"Test execution failed: {e}")
    if errors:
        conn.close()
        return {"status": "error", "errors": errors}
    if validate_only:
        conn.close()
        return {"status": "valid", "indicator_id": ind_id, "errors": []}
    # Save
    try:
        conn.execute(
            """INSERT INTO indicators (indicator_id, name, display_name, category, description,
               output_type, output_labels, params, calc_code, usage_example, inputs, created_by, is_builtin)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)""",
            (ind_id, body.get("name",""), body.get("display_name", body.get("name","")),
             body.get("category","custom"), body.get("description",""),
             body.get("output_type","single"), json.dumps(body.get("output_labels",["main"])),
             json.dumps(body.get("params",[])), calc_code,
             body.get("usage_example",""), json.dumps(body.get("inputs",["closes"])),
             body.get("created_by","unknown")))
        conn.commit()
    finally:
        conn.close()
    return {"status": "registered", "indicator_id": ind_id,
            "message": f"{body.get('name',ind_id)} registered. Available in strategy builder."}


@app.delete("/api/indicators/{indicator_id}")
async def delete_indicator(indicator_id: str):
    conn = get_db()
    try:
        row = conn.execute("SELECT is_builtin FROM indicators WHERE indicator_id=?", (indicator_id.upper(),)).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        if row["is_builtin"]:
            raise HTTPException(403, "Cannot delete built-in indicator")
        conn.execute("DELETE FROM indicators WHERE indicator_id=?", (indicator_id.upper(),))
        conn.commit()
        return {"status": "deleted", "indicator_id": indicator_id}
    finally:
        conn.close()


@app.post("/api/indicators/validate")
async def indicators_validate(file: UploadFile = File(...), email: str = Form("")):
    """Validate a .quantx indicator file without saving."""
    from api.indicator_validator import validate_quantx_file
    try:
        raw = await file.read()
        data = json.loads(raw)
    except Exception:
        return {"status": "error", "stage": "json_parse", "message": "Invalid JSON file"}
    conn = get_db()
    try:
        return validate_quantx_file(data, email, conn)
    finally:
        conn.close()


@app.post("/api/indicators/import")
async def indicators_import(file: UploadFile = File(...), email: str = Form(""),
                            overwrite: bool = Form(False)):
    """Validate and register a .quantx indicator file."""
    from api.indicator_validator import validate_quantx_file
    from api.database import register_custom_indicator
    try:
        raw = await file.read()
        data = json.loads(raw)
    except Exception:
        return {"status": "error", "stage": "json_parse", "message": "Invalid JSON file"}
    conn = get_db()
    try:
        result = validate_quantx_file(data, email, conn)
        if result["status"] == "error":
            return result
        if result["status"] == "exists" and not overwrite:
            return result
        ind_id = register_custom_indicator(conn, data, email, overwrite=(result["status"] == "exists"))
        return {"status": "registered", "indicator_id": ind_id, "name": data.get("name", ind_id),
                "preview": result.get("preview"), "warmup_detected": result.get("warmup_detected")}
    finally:
        conn.close()


@app.get("/api/status/{email}")
async def status(email: str):
    email = email.lower().strip()

    # Check DB for LP and IBKR processes
    conn = get_db()
    try:
        lp_proc = conn.execute(
            "SELECT pid, status FROM processes WHERE email=? AND master_script_path NOT LIKE '%ibkr%' ORDER BY id DESC LIMIT 1",
            (email,)
        ).fetchone()
        ibkr_proc = conn.execute(
            "SELECT pid, status FROM processes WHERE email=? AND master_script_path LIKE '%ibkr%' ORDER BY id DESC LIMIT 1",
            (email,)
        ).fetchone()
    finally:
        conn.close()

    def _is_alive(proc_row):
        if not proc_row or proc_row["status"] != "running":
            return False
        try:
            import psutil
            p = psutil.Process(proc_row["pid"])
            return p.is_running() and p.status() != "zombie"
        except Exception:
            return False

    # Also check in-memory processes
    lp_running = _is_alive(lp_proc)
    ibkr_running = _is_alive(ibkr_proc)

    # Fallback: check _running_processes dict
    if not lp_running and email in _running_processes:
        proc = _running_processes[email]
        if proc.poll() is None:
            lp_running = True

    strategies = get_strategies(email)
    trades = get_trades(email)
    strat_pnl = defaultdict(float)
    for t in trades:
        strat_pnl[t["strategy_id"]] += t["pnl"]

    return {
        "email": email,
        "status": "running" if (lp_running or ibkr_running) else "stopped",
        "lp_running": lp_running,
        "lp_pid": lp_proc["pid"] if lp_proc and lp_running else None,
        "ibkr_running": ibkr_running,
        "ibkr_pid": ibkr_proc["pid"] if ibkr_proc and ibkr_running else None,
        "pid": (lp_proc["pid"] if lp_running else None) or (ibkr_proc["pid"] if ibkr_running else None),
        "is_running": lp_running or ibkr_running,
        "bot_status": "running" if (lp_running or ibkr_running) else "stopped",
        "strategies": [
            {**s, "total_pnl": round(strat_pnl.get(s["strategy_id"], 0), 4)}
            for s in strategies
        ],
        "total_pnl": round(sum(strat_pnl.values()), 4),
        "total_trades": len(trades),
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
        ctx = get_lp_quote_ctx(student["app_key"], student["app_secret"], student["access_token"])
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


@app.get("/api/ticker/search")
async def ticker_search(q: str = Query(""), email: str = Query("")):
    """Search tickers by symbol, name, or alias.
    Falls back to LongPort static_info if not found locally."""
    from api.ticker_search import search_ticker
    lp_creds = None
    if email:
        student = get_student(email.lower().strip())
        if student and student.get("app_key"):
            lp_creds = {"app_key": student["app_key"],
                        "app_secret": student["app_secret"],
                        "access_token": student["access_token"]}

    def _search():
        return search_ticker(q, lp_creds, limit=10)

    results = await asyncio.get_event_loop().run_in_executor(_executor, _search)
    return {"results": results, "query": q}


@app.get("/api/ticker/validate")
async def ticker_validate(symbol: str = Query(""), email: str = Query("")):
    """Directly validate a symbol via LongPort static_info."""
    if not symbol or not email:
        return {"valid": False, "result": None}
    student = get_student(email.lower().strip())
    lp_creds = None
    if student and student.get("app_key"):
        lp_creds = {"app_key": student["app_key"],
                    "app_secret": student["app_secret"],
                    "access_token": student["access_token"]}

    def _validate():
        from api.ticker_search import lookup_lp
        sym = symbol.upper().strip()
        candidates = [sym]
        if not (sym.endswith(".US") or sym.endswith(".HK") or sym.endswith(".SI")):
            candidates = [f"{sym}.US", f"{sym}.HK", sym]
        return lookup_lp(candidates, lp_creds or {})

    results = await asyncio.get_event_loop().run_in_executor(_executor, _validate)
    if results:
        return {"valid": True, "result": results[0]}
    return {"valid": False, "result": None}


@app.post("/api/test-connection")
async def test_connection(req: DeployReq):
    email = req.email.lower().strip()
    student = get_student(email)
    if not student:
        raise HTTPException(404, "Student not registered")
    def _test_lp():
        try:
            ctx = get_lp_quote_ctx(student["app_key"], student["app_secret"], student["access_token"])
            quotes = ctx.quote(["700.HK"])
            if quotes:
                q = quotes[0]
                return {"ok": True, "message": "Connected to LongPort",
                        "test_quote": {"symbol": "700.HK", "price": float(q.last_done)}}
            return {"ok": True, "message": "Connected but no quote returned", "test_quote": None}
        except Exception as e:
            return {"ok": False, "message": f"Connection failed: {e}"}
    result = await asyncio.get_event_loop().run_in_executor(_executor, _test_lp)
    return result


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
