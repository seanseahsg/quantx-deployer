"""QuantX — IBKR bot template string for code generation."""

IBKR_BOT_TEMPLATE = r'''#!/usr/bin/env python3
"""QuantX IBKR Bot — Auto-generated. DO NOT EDIT MANUALLY."""

import os
import sys
import time
import json
import signal
import logging
import requests
import threading
from datetime import datetime, timezone, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
EMAIL = '{email}'
STUDENT_NAME = '{student_name}'
CENTRAL_API_URL = '{central_api_url}'
IBKR_HOST = '{ibkr_host}'
IBKR_PORT = {ibkr_port}
IBKR_CLIENT_ID = {ibkr_client_id}
LOG_DIR = '{log_dir}'
MASTER_LOG_NAME = '{master_log_name}'

STRATEGIES = {strategies_json}

HEARTBEAT_INTERVAL = 60
SGT = timezone(timedelta(hours=8))

# ── Logging ─────────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, MASTER_LOG_NAME), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
master_log = logging.getLogger("quantx-ibkr")

_shutdown = threading.Event()

def _handle_signal(signum, frame):
    master_log.warning("Signal %s — shutting down", signum)
    _shutdown.set()

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Strategy Runner ──────────────────────────────────────────────────────────
class IBKRStrategyRunner:
    def __init__(self, strategy, connector):
        self.strategy = strategy
        self.connector = connector
        self.strategy_id = strategy['strategy_id']
        self.symbol = strategy['symbol']
        self.risk = strategy.get('risk', {{}})
        self.lots = int(self.risk.get('lots', 1))
        self.sl_pct = float(self.risk.get('sl_pct', 5))
        self.tp_pct = float(self.risk.get('tp_pct', 10))
        self.in_position = False
        self.entry_price = None
        master_log.info("IBKRRunner init: %s %s lots=%d", self.strategy_id, self.symbol, self.lots)

    def check_and_trade(self):
        try:
            price = self.connector.get_quote(self.symbol)
            if not price:
                master_log.warning("%s: no price available", self.symbol)
                return
            master_log.info("%s price: %.2f", self.symbol, price)

            if not self.in_position:
                master_log.info("%s: watching for entry signal", self.symbol)
            else:
                if self.entry_price:
                    pnl_pct = (price - self.entry_price) / self.entry_price * 100
                    if pnl_pct >= self.tp_pct:
                        master_log.info("%s: TP hit %.2f%%, selling", self.symbol, pnl_pct)
                        self.connector.place_market_order(self.symbol, 'SELL', self.lots)
                        self._report_trade('SELL', price, self.lots)
                        self.in_position = False
                        self.entry_price = None
                    elif pnl_pct <= -self.sl_pct:
                        master_log.info("%s: SL hit %.2f%%, selling", self.symbol, pnl_pct)
                        self.connector.place_market_order(self.symbol, 'SELL', self.lots)
                        self._report_trade('SELL', price, self.lots)
                        self.in_position = False
                        self.entry_price = None
        except Exception as e:
            master_log.error("Runner error %s: %s", self.symbol, e)

    def _report_trade(self, side, price, qty):
        try:
            pnl = 0
            if side == 'SELL' and self.entry_price:
                pnl = (price - self.entry_price) * qty
            if CENTRAL_API_URL:
                requests.post(f"{{CENTRAL_API_URL}}/api/trade", json={{
                    "email": EMAIL, "student_name": STUDENT_NAME,
                    "strategy_id": self.strategy_id, "symbol": self.symbol,
                    "side": side, "price": price, "qty": qty, "pnl": pnl,
                }}, timeout=10)
        except Exception as e:
            master_log.warning("Trade report failed: %s", e)

    def status_dict(self):
        return {{
            "strategy_id": self.strategy_id, "symbol": self.symbol,
            "in_position": self.in_position, "mode": "ibkr",
        }}

# ── Heartbeat ────────────────────────────────────────────────────────────────
def heartbeat_loop(runners):
    while not _shutdown.is_set():
        _shutdown.wait(HEARTBEAT_INTERVAL)
        if _shutdown.is_set():
            break
        try:
            if CENTRAL_API_URL:
                requests.post(f"{{CENTRAL_API_URL}}/api/heartbeat", json={{
                    "email": EMAIL, "vps_ip": "local", "bot_status": "running",
                    "strategies": [r.status_dict() for r in runners],
                }}, timeout=10)
        except Exception:
            pass

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    master_log.info("=" * 60)
    master_log.info("QuantX IBKR Bot — %s", EMAIL)
    master_log.info("Strategies: %d", len(STRATEGIES))
    master_log.info("IBKR: %s:%s clientId=%s", IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID)
    master_log.info("=" * 60)

    try:
        from api.ibkr_connector import IBKRConnector
    except ImportError:
        master_log.error("IBKRConnector not found. pip install ib_insync")
        return

    connector = IBKRConnector(IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID)
    if not connector.connect():
        master_log.error("Failed to connect to IBKR at %s:%s", IBKR_HOST, IBKR_PORT)
        master_log.error("Make sure TWS or IB Gateway is running.")
        return

    master_log.info("IBKR connected successfully")
    runners = [IBKRStrategyRunner(s, connector) for s in STRATEGIES]
    if not runners:
        master_log.error("No strategies. Exiting.")
        return

    hb = threading.Thread(target=heartbeat_loop, args=(runners,), daemon=True)
    hb.start()

    POLL_INTERVAL = 60
    master_log.info("Trading loop started. Polling every %ds", POLL_INTERVAL)

    while not _shutdown.is_set():
        now_sgt = datetime.now(SGT).strftime("%H:%M:%S SGT")
        master_log.info("── Tick %s ──", now_sgt)
        for runner in runners:
            runner.check_and_trade()
        _shutdown.wait(POLL_INTERVAL)

    master_log.info("Shutdown complete")
    connector.disconnect()

if __name__ == '__main__':
    main()
'''
