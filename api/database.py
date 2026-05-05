"""QuantX Deployer — SQLite database with Fernet encryption for credentials."""

import os
import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path
from cryptography.fernet import Fernet

from .config import DB_PATH, KEY_FILE, FERNET_KEY


def _get_or_create_key() -> bytes:
    # Priority 1: environment variable (Railway)
    if FERNET_KEY:
        return FERNET_KEY.encode() if isinstance(FERNET_KEY, str) else FERNET_KEY
    # Priority 2: key file on disk (VPS)
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            return f.read()
    # Priority 3: generate new key and save to file
    key = Fernet.generate_key()
    os.makedirs(os.path.dirname(KEY_FILE) or ".", exist_ok=True)
    with open(KEY_FILE, "wb") as f:
        f.write(key)
    return key


_fernet = Fernet(_get_or_create_key())


def encrypt(value: str) -> str:
    if not value:
        return ""
    return _fernet.encrypt(value.encode()).decode()


def decrypt(token: str) -> str:
    if not token:
        return ""
    return _fernet.decrypt(token.encode()).decode()


import threading
_local = threading.local()


def get_db() -> sqlite3.Connection:
    """Get a thread-local SQLite connection with performance tuning."""
    if hasattr(_local, "conn") and _local.conn is not None:
        try:
            _local.conn.execute("SELECT 1")
            return _local.conn
        except Exception:
            _local.conn = None
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA temp_store=memory")
    conn.execute("PRAGMA foreign_keys=ON")
    _local.conn = conn
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            name TEXT DEFAULT '',
            app_key_enc TEXT DEFAULT '',
            app_secret_enc TEXT DEFAULT '',
            access_token_enc TEXT DEFAULT '',
            central_api_url TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS strategies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            strategy_id TEXT UNIQUE NOT NULL,
            strategy_name TEXT DEFAULT '',
            symbol TEXT DEFAULT '',
            arena TEXT DEFAULT 'US',
            timeframe TEXT DEFAULT '1m',
            conditions_json TEXT DEFAULT '{}',
            exit_rules_json TEXT DEFAULT '{}',
            risk_json TEXT DEFAULT '{}',
            is_active INTEGER DEFAULT 1,
            mode TEXT NOT NULL DEFAULT 'library',
            library_id TEXT DEFAULT '',
            custom_script TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (email) REFERENCES students(email)
        );

        CREATE TABLE IF NOT EXISTS processes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            pid INTEGER DEFAULT 0,
            status TEXT DEFAULT 'stopped',
            master_script_path TEXT DEFAULT '',
            log_path TEXT DEFAULT '',
            started_at TEXT,
            stopped_at TEXT,
            error_msg TEXT DEFAULT '',
            FOREIGN KEY (email) REFERENCES students(email)
        );

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            strategy_id TEXT DEFAULT '',
            symbol TEXT DEFAULT '',
            side TEXT DEFAULT '',
            price REAL DEFAULT 0,
            qty REAL DEFAULT 0,
            pnl REAL DEFAULT 0,
            cumulative_pnl REAL DEFAULT 0,
            timestamp TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (email) REFERENCES students(email)
        );
    """)
    conn.commit()
    # Migration: add new strategy columns
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(strategies)").fetchall()}
        for col, default in [("allocation", "'10000'"), ("backtest_results_json", "NULL"),
                              ("live_results_json", "NULL"), ("trade_log_json", "NULL"),
                              ("broker", "'longport'")]:
            if col not in cols:
                conn.execute(f"ALTER TABLE strategies ADD COLUMN {col} TEXT DEFAULT {default}")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE strategies ADD COLUMN is_dry_run INTEGER DEFAULT 0")
        conn.commit()
    except Exception:
        pass
    # Data cache table
    try:
        from .data_manager import init_data_cache
        init_data_cache(conn)
    except Exception:
        pass
    # Indicators table
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                category TEXT DEFAULT 'custom',
                description TEXT DEFAULT '',
                output_type TEXT DEFAULT 'single',
                output_labels TEXT DEFAULT '["main"]',
                params TEXT DEFAULT '[]',
                calc_code TEXT NOT NULL,
                usage_example TEXT DEFAULT '',
                pine_script_equivalent TEXT DEFAULT '',
                tradingview_name TEXT DEFAULT '',
                created_by TEXT DEFAULT 'system',
                created_at TEXT DEFAULT (datetime('now')),
                is_builtin INTEGER DEFAULT 0,
                is_approved INTEGER DEFAULT 1,
                usage_count INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_indicators_category ON indicators(category);
        """)
        conn.commit()
        from .indicators_seed import seed_builtin_indicators
        seed_builtin_indicators(conn)
    except Exception:
        pass
    # Indicator table migrations
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(indicators)").fetchall()}
        if "source" not in cols:
            conn.execute("ALTER TABLE indicators ADD COLUMN source TEXT DEFAULT ''")
        if "inputs" not in cols:
            conn.execute("ALTER TABLE indicators ADD COLUMN inputs TEXT DEFAULT '[\"closes\"]'")
        conn.commit()
    except Exception:
        pass
    # Broker accounts table
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS broker_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                broker TEXT NOT NULL,
                account_type TEXT NOT NULL DEFAULT 'paper',
                account_id TEXT DEFAULT '',
                nickname TEXT DEFAULT '',
                app_key_enc TEXT DEFAULT '',
                app_secret_enc TEXT DEFAULT '',
                access_token_enc TEXT DEFAULT '',
                is_connected INTEGER DEFAULT 0,
                last_tested TEXT DEFAULT '',
                last_error TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(email, broker, account_type)
            );
        """)
        conn.commit()
        # Migrate existing LongPort credentials
        _migrate_broker_accounts(conn)
    except Exception:
        pass
    conn.close()


def _migrate_broker_accounts(conn):
    """One-time migration: copy existing credentials into broker_accounts."""
    try:
        existing = conn.execute("SELECT COUNT(*) FROM broker_accounts").fetchone()[0]
        if existing > 0:
            return  # already migrated
        # Migrate LongPort from students table
        rows = conn.execute(
            "SELECT email, app_key_enc, app_secret_enc, access_token_enc FROM students WHERE app_key_enc != ''"
        ).fetchall()
        for r in rows:
            conn.execute(
                """INSERT OR IGNORE INTO broker_accounts
                   (email, broker, account_type, nickname, app_key_enc, app_secret_enc, access_token_enc)
                   VALUES (?, 'longport', 'paper', 'LongPort Demo', ?, ?, ?)""",
                (r[0], r[1], r[2], r[3]))
        conn.commit()
    except Exception:
        pass


# ── Student helpers ─────────────────────────────────────────────────────────

def save_student(email: str, name: str, app_key: str, app_secret: str,
                 access_token: str, central_api_url: str):
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO students (email, name, app_key_enc, app_secret_enc, access_token_enc, central_api_url)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(email) DO UPDATE SET
                 name=excluded.name,
                 app_key_enc=excluded.app_key_enc,
                 app_secret_enc=excluded.app_secret_enc,
                 access_token_enc=excluded.access_token_enc,
                 central_api_url=excluded.central_api_url""",
            (email, name, encrypt(app_key), encrypt(app_secret),
             encrypt(access_token), central_api_url),
        )
        conn.commit()
    finally:
        conn.close()


def get_student(email: str) -> dict | None:
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM students WHERE email = ?", (email,)).fetchone()
        if not row:
            return None
        return {
            "email": row["email"],
            "name": row["name"],
            "app_key": decrypt(row["app_key_enc"]),
            "app_secret": decrypt(row["app_secret_enc"]),
            "access_token": decrypt(row["access_token_enc"]),
            "central_api_url": row["central_api_url"],
            "created_at": row["created_at"],
        }
    finally:
        conn.close()


# ── Strategy helpers ────────────────────────────────────────────────────────

def save_strategy(email: str, strategy_id: str, strategy_name: str, symbol: str,
                  arena: str, timeframe: str, conditions: dict, exit_rules: dict,
                  risk: dict, is_active: bool = True, mode: str = "library",
                  library_id: str = "", custom_script: str = "",
                  broker: str = "longport", is_dry_run: bool = False):
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO strategies (email, strategy_id, strategy_name, symbol, arena, timeframe,
                                      conditions_json, exit_rules_json, risk_json, is_active,
                                      mode, library_id, custom_script, broker, is_dry_run)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(strategy_id) DO UPDATE SET
                 strategy_name=excluded.strategy_name,
                 symbol=excluded.symbol,
                 arena=excluded.arena,
                 timeframe=excluded.timeframe,
                 conditions_json=excluded.conditions_json,
                 exit_rules_json=excluded.exit_rules_json,
                 risk_json=excluded.risk_json,
                 is_active=excluded.is_active,
                 mode=excluded.mode,
                 library_id=excluded.library_id,
                 custom_script=excluded.custom_script,
                 broker=excluded.broker,
                 is_dry_run=excluded.is_dry_run""",
            (email, strategy_id, strategy_name, symbol, arena, timeframe,
             json.dumps(conditions), json.dumps(exit_rules), json.dumps(risk),
             1 if is_active else 0, mode, library_id, custom_script, broker,
             1 if is_dry_run else 0),
        )
        conn.commit()
    finally:
        conn.close()


def get_strategies(email: str, active_only: bool = False) -> list[dict]:
    conn = get_db()
    try:
        q = "SELECT * FROM strategies WHERE email = ?"
        if active_only:
            q += " AND is_active = 1"
        rows = conn.execute(q, (email,)).fetchall()
        result = []
        for r in rows:
            rk = r.keys()
            d = {
                "strategy_id": r["strategy_id"],
                "strategy_name": r["strategy_name"],
                "symbol": r["symbol"],
                "arena": r["arena"],
                "timeframe": r["timeframe"],
                "conditions": json.loads(r["conditions_json"]),
                "exit_rules": json.loads(r["exit_rules_json"]),
                "risk": json.loads(r["risk_json"]),
                "is_active": bool(r["is_active"]),
                "created_at": r["created_at"],
                "mode": r["mode"] if "mode" in rk else "library",
                "library_id": r["library_id"] if "library_id" in rk else "",
                "custom_script": r["custom_script"] if "custom_script" in rk else "",
                "broker": r["broker"] if "broker" in rk else "longport",
                "allocation": float(r["allocation"]) if "allocation" in rk and r["allocation"] else 10000,
                "backtest_results": json.loads(r["backtest_results_json"]) if "backtest_results_json" in rk and r["backtest_results_json"] else None,
                "live_results": json.loads(r["live_results_json"]) if "live_results_json" in rk and r["live_results_json"] else None,
                "trade_log": json.loads(r["trade_log_json"]) if "trade_log_json" in rk and r["trade_log_json"] else [],
                "is_dry_run": bool(r["is_dry_run"]) if "is_dry_run" in rk else False,
            }
            result.append(d)
        return result
    finally:
        conn.close()


def delete_strategy(strategy_id: str) -> bool:
    conn = get_db()
    try:
        cur = conn.execute("DELETE FROM strategies WHERE strategy_id = ?", (strategy_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def toggle_strategy(strategy_id: str, is_active: bool):
    conn = get_db()
    try:
        conn.execute("UPDATE strategies SET is_active = ? WHERE strategy_id = ?",
                      (1 if is_active else 0, strategy_id))
        conn.commit()
    finally:
        conn.close()


# ── Process helpers ─────────────────────────────────────────────────────────

def save_process(email: str, pid: int, status: str, script_path: str, log_path: str):
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO processes (email, pid, status, master_script_path, log_path, started_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (email, pid, status, script_path, log_path,
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def update_process_status(email: str, status: str, error_msg: str = ""):
    conn = get_db()
    try:
        conn.execute(
            """UPDATE processes SET status = ?, stopped_at = ?, error_msg = ?
               WHERE email = ? AND id = (SELECT MAX(id) FROM processes WHERE email = ?)""",
            (status, datetime.now(timezone.utc).isoformat(), error_msg, email, email),
        )
        conn.commit()
    finally:
        conn.close()


def get_latest_process(email: str) -> dict | None:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM processes WHERE email = ? ORDER BY id DESC LIMIT 1", (email,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ── Trade helpers ───────────────────────────────────────────────────────────

def log_trade(email: str, strategy_id: str, symbol: str, side: str,
              price: float, qty: float, pnl: float) -> float:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) as total FROM trades WHERE email = ? AND strategy_id = ?",
            (email, strategy_id),
        ).fetchone()
        cum_pnl = (row["total"] if row else 0) + pnl
        conn.execute(
            """INSERT INTO trades (email, strategy_id, symbol, side, price, qty, pnl, cumulative_pnl, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (email, strategy_id, symbol, side, price, qty, pnl, cum_pnl,
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return cum_pnl
    finally:
        conn.close()


def get_trades(email: str) -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM trades WHERE email = ? ORDER BY timestamp DESC LIMIT 500", (email,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Broker accounts helpers ────────────────────────────────────────────────

def get_broker_accounts(email: str) -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM broker_accounts WHERE email = ? ORDER BY broker, account_type",
            (email,)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            # Don't expose encrypted credentials
            d.pop("app_key_enc", None)
            d.pop("app_secret_enc", None)
            d.pop("access_token_enc", None)
            result.append(d)
        return result
    finally:
        conn.close()


def get_broker_account(account_id: int) -> dict | None:
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM broker_accounts WHERE id = ?", (account_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def save_broker_account(email: str, broker: str, account_type: str,
                        nickname: str = "", account_id: str = "",
                        app_key: str = "", app_secret: str = "",
                        access_token: str = "") -> int:
    conn = get_db()
    try:
        # Encrypt LP credentials if provided
        ak_enc = encrypt(app_key) if app_key else ""
        as_enc = encrypt(app_secret) if app_secret else ""
        at_enc = encrypt(access_token) if access_token else ""
        conn.execute(
            """INSERT INTO broker_accounts
               (email, broker, account_type, nickname, account_id,
                app_key_enc, app_secret_enc, access_token_enc)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(email, broker, account_type) DO UPDATE SET
                 nickname=excluded.nickname, account_id=excluded.account_id,
                 app_key_enc=excluded.app_key_enc, app_secret_enc=excluded.app_secret_enc,
                 access_token_enc=excluded.access_token_enc""",
            (email, broker, account_type, nickname, account_id,
             ak_enc, as_enc, at_enc))
        conn.commit()
        row = conn.execute(
            "SELECT id FROM broker_accounts WHERE email=? AND broker=? AND account_type=?",
            (email, broker, account_type)).fetchone()
        return row["id"] if row else 0
    finally:
        conn.close()


def update_broker_account_status(account_id: int, is_connected: bool,
                                  last_error: str = ""):
    conn = get_db()
    try:
        conn.execute(
            """UPDATE broker_accounts SET is_connected=?, last_tested=datetime('now'),
               last_error=? WHERE id=?""",
            (1 if is_connected else 0, last_error, account_id))
        conn.commit()
    finally:
        conn.close()


def delete_broker_account(account_id: int) -> bool:
    conn = get_db()
    try:
        cur = conn.execute("DELETE FROM broker_accounts WHERE id = ?", (account_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_broker_credentials(account_id: int) -> dict | None:
    """Get decrypted credentials for a broker account."""
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM broker_accounts WHERE id = ?", (account_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("app_key_enc"):
            d["app_key"] = decrypt(d["app_key_enc"])
            d["app_secret"] = decrypt(d["app_secret_enc"])
            d["access_token"] = decrypt(d["access_token_enc"])
        return d
    finally:
        conn.close()


# ── Custom indicator helpers ───────────────────────────────────────────────

def register_custom_indicator(conn, data: dict, email: str,
                              overwrite: bool = False) -> str:
    """Register a custom indicator from .quantx file data. Returns indicator_id."""
    ind_id = data["indicator_id"]
    calc_code = "\n".join(data["calc_code"]) if isinstance(data.get("calc_code"), list) else data.get("calc_code", "")
    if overwrite:
        conn.execute("DELETE FROM indicators WHERE indicator_id = ? AND is_builtin = 0", (ind_id,))
    conn.execute(
        """INSERT OR REPLACE INTO indicators
           (indicator_id, name, display_name, category, description,
            output_type, output_labels, params, calc_code, usage_example,
            inputs, source, created_by, is_builtin, is_approved)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0,1)""",
        (ind_id, data.get("name", ind_id), data.get("display_name", data.get("name", ind_id)),
         data.get("category", "custom"), data.get("description", ""),
         data.get("output_type", "single"),
         json.dumps(data.get("output_labels", ["main"])),
         json.dumps(data.get("params", [])),
         calc_code,
         data.get("usage_example", f"calc_{ind_id.lower()}(closes, ...)"),
         json.dumps(data.get("inputs", ["closes"])),
         data.get("source", ""),
         email))
    conn.commit()
    return ind_id


def get_custom_indicators(conn, email: str = None) -> list:
    """Return all non-builtin indicators as dicts.
    On a local app there's no multi-tenancy, so return all custom indicators."""
    rows = conn.execute(
        "SELECT * FROM indicators WHERE is_builtin = 0 ORDER BY name").fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["output_labels"] = json.loads(d.get("output_labels") or "[]")
        d["params"] = json.loads(d.get("params") or "[]")
        d["inputs"] = json.loads(d.get("inputs") or '["closes"]')
        result.append(d)
    return result
