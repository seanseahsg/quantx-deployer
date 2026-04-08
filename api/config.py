"""QuantX Deployer — Centralized configuration.
Works on both Windows VPS and Railway Linux.
"""

import os
import sys
from pathlib import Path

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

# Central API
CENTRAL_API_URL = os.environ.get("CENTRAL_API_URL", "http://localhost:8001")

# Admin
ADMIN_PIN = os.environ.get("ADMIN_PIN", "quantx2025")

# Hosting mode
HOSTING = os.environ.get("HOSTING", "vps")  # "vps" or "railway"

# FMP API
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")

# Version
VERSION = "1.0.0"

# Python executable
PYTHON_EXE = sys.executable

# Ensure directories exist
BOTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
TRADES_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
