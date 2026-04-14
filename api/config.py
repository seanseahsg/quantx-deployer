"""QuantX Deployer — Centralized configuration.
Works on both Windows VPS and Railway Linux.

Architecture:
  RAILWAY (remote) = backtesting only (FMP data, R2 cache)
  LOCAL            = everything else (deploy, trade, broker, logs)
"""

import os
import sys
from pathlib import Path

# Load .env file if it exists (for student installs)
_env_file = Path(__file__).parent.parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

# Base directory
BASE_DIR = Path(os.environ.get("APP_BASE_DIR", str(Path(__file__).parent.parent)))

# Data directory — use Railway volume or local
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR)))

# Database
DB_PATH = os.environ.get("DB_PATH", str(DATA_DIR / "quantx_deployer.db"))

# Key file for Fernet encryption
KEY_FILE = str(DATA_DIR / ".fernet.key")

# Fernet key from env (Railway) or file (VPS)
FERNET_KEY = os.environ.get("FERNET_KEY", "")

# Generated scripts and logs
BOTS_DIR = DATA_DIR / "bots"
LOGS_DIR = DATA_DIR / "logs"
TRADES_DIR = DATA_DIR / "trades"
STATE_DIR = DATA_DIR / "state"

# ── Architecture split ──────────────────────────────────────────────────────
# Railway (remote) — backtesting only
CENTRAL_API_URL = os.environ.get("CENTRAL_API_URL", "")
BACKTEST_URL = CENTRAL_API_URL  # all backtest calls route here when set

# Local — everything else (deploy, stop, strategies, trades, logs, brokers)
LOCAL_API_PORT = int(os.environ.get("PORT", 8080))

# Admin
ADMIN_PIN = os.environ.get("ADMIN_PIN", "quantx2025")

# Hosting mode
HOSTING = os.environ.get("HOSTING", "vps")  # "vps" or "railway"

# FMP API
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")

# Version
VERSION = "1.1.0"

# Python executable
PYTHON_EXE = sys.executable

# Ensure directories exist
BOTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
TRADES_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
