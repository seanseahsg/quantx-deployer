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


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
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
    conn.close()


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
                  library_id: str = "", custom_script: str = ""):
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO strategies (email, strategy_id, strategy_name, symbol, arena, timeframe,
                                      conditions_json, exit_rules_json, risk_json, is_active,
                                      mode, library_id, custom_script)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                 custom_script=excluded.custom_script""",
            (email, strategy_id, strategy_name, symbol, arena, timeframe,
             json.dumps(conditions), json.dumps(exit_rules), json.dumps(risk),
             1 if is_active else 0, mode, library_id, custom_script),
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
                "mode": r["mode"] if "mode" in r.keys() else "library",
                "library_id": r["library_id"] if "library_id" in r.keys() else "",
                "custom_script": r["custom_script"] if "custom_script" in r.keys() else "",
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
            "SELECT * FROM trades WHERE email = ? ORDER BY timestamp DESC", (email,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
