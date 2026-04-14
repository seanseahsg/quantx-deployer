#!/usr/bin/env python3
"""QuantX Deployer -- start the app.

Architecture:
  Backtesting = Railway (remote) if CENTRAL_API_URL is set, else local (Yahoo)
  Trading     = always local (your IBKR / LongPort)
"""
import os
import sys
import subprocess
import webbrowser
import time
from pathlib import Path

BASE = Path(__file__).parent


def load_env():
    env_file = BASE / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def main():
    load_env()
    port = int(os.environ.get("PORT", 8080))
    central = os.environ.get("CENTRAL_API_URL", "")

    print("=" * 50)
    print("  QuantX Deployer v1.1")
    print(f"  App: http://localhost:{port}")
    if central:
        print(f"  Backtest server: {central}")
    else:
        print("  Backtest server: local (Yahoo Finance)")
    print("  Trading: local (your IBKR / LongPort)")
    print("=" * 50)

    python = sys.executable
    proc = subprocess.Popen(
        [python, "-m", "uvicorn", "api.main:app",
         "--host", "127.0.0.1", f"--port={port}",
         "--reload",
         "--reload-exclude=bots/*", "--reload-exclude=logs/*",
         "--reload-exclude=*.db", "--reload-exclude=*.log"],
        cwd=str(BASE),
    )

    time.sleep(2)
    webbrowser.open(f"http://localhost:{port}")

    try:
        proc.wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
        proc.terminate()


if __name__ == "__main__":
    main()
