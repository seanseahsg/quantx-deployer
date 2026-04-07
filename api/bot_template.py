"""QuantX Deployer — Unified multi-strategy master bot template.
Supports both standard indicator strategies AND SYMMETRIC_GRID strategies
in a single process with shared QuoteContext/TradeContext.
"""

BOT_TEMPLATE = r'''#!/usr/bin/env python3
"""QuantX Master Bot — Auto-generated unified trading bot.
Supports: standard indicators, custom scripts, and grid strategies.
DO NOT EDIT MANUALLY. Regenerated on each deploy.
"""

import os
import sys
import json
import math
import time
import signal
import logging
import threading
from datetime import datetime, timezone, timedelta
from collections import deque

import httpx

# ── Configuration ──────────────────────────────────────────────────────────

EMAIL = {email!r}
STUDENT_NAME = {student_name!r}
CENTRAL_API_URL = {central_api_url!r}
LOCAL_API_URL = "http://127.0.0.1:8080"
APP_KEY = {app_key!r}
APP_SECRET = {app_secret!r}
ACCESS_TOKEN = {access_token!r}
LOG_DIR = {log_dir!r}

STRATEGIES = {strategies_json}

HEARTBEAT_INTERVAL = 60
SGT = timezone(timedelta(hours=8))

ARENA_LOT_SIZE = {{"US": 1, "HK": 100, "HK_ETF": 500, "SG": 100}}

# ── Logging ────────────────────────────────────────────────────────────────

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, {master_log_name!r}), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
master_log = logging.getLogger("quantx-master")

_shutdown = threading.Event()

def _handle_signal(signum, frame):
    master_log.warning("Signal %s — shutting down", signum)
    _shutdown.set()

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── Helpers ────────────────────────────────────────────────────────────────

def hk_tick_round(price, side="buy", arena="HK"):
    if arena != "HK":
        return round(price, 2)
    if price < 0.25: tick = 0.001
    elif price < 0.50: tick = 0.005
    elif price < 10: tick = 0.010
    elif price < 20: tick = 0.020
    elif price < 100: tick = 0.050
    elif price < 200: tick = 0.100
    elif price < 500: tick = 0.200
    elif price < 1000: tick = 0.500
    elif price < 2000: tick = 1.000
    elif price < 5000: tick = 2.000
    else: tick = 5.000
    if side == "buy":
        return round(math.floor(price / tick) * tick, 4)
    return round(math.ceil(price / tick) * tick, 4)


def is_market_open(arena):
    now = datetime.now(SGT)
    h, m = now.hour, now.minute
    if arena == "US":
        return (h > 21 or (h == 21 and m >= 30) or h < 4), "US 9:30pm-4:00am SGT"
    if arena in ("HK", "HK_ETF"):
        return ((h > 9 or (h == 9 and m >= 30)) and h < 16), "HK 9:30am-4:00pm SGT"
    return True, "Unknown"


def post_json(url, payload):
    try:
        httpx.post(url, json=payload, timeout=5)
    except Exception:
        pass


# ── BaseRunner ─────────────────────────────────────────────────────────────

class BaseRunner:
    """Common interface for all strategy runners."""

    def __init__(self, config, trade_ctx):
        self.strategy_id = config["strategy_id"]
        self.strategy_name = config.get("strategy_name", self.strategy_id)
        self.symbol = config.get("symbol", "")
        self.arena = config.get("arena", "US")
        self.mode = config.get("mode", "library")
        self.library_id = config.get("library_id", "")
        self.risk = config.get("risk", {{}})
        self.trade_ctx = trade_ctx

        self.position_qty = 0
        self.cumulative_pnl = 0.0
        self.status = "waiting"
        self.tracked_order_ids = set()

        # Per-strategy log file
        safe_sid = self.strategy_id.replace("/", "_")
        email_safe = EMAIL.replace("@", "_at_").replace(".", "_")
        self.log_path = os.path.join(LOG_DIR, f"{{email_safe}}_{{safe_sid}}.log")

    def log(self, msg):
        ts = datetime.now(SGT).strftime("%H:%M:%S.%f")[:-3]
        line = f"[{{ts}}][{{self.strategy_id}}] {{msg}}"
        print(line, flush=True)
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def on_tick(self, price):
        raise NotImplementedError

    def on_fill(self, order_id, fill_price, fill_qty, side_str):
        raise NotImplementedError

    def shutdown(self, last_price):
        pass

    def report_trade(self, side, price, qty, pnl):
        self.cumulative_pnl += pnl
        post_json(f"{{LOCAL_API_URL}}/api/trade", {{
            "email": EMAIL, "student_name": STUDENT_NAME,
            "strategy_id": self.strategy_id, "symbol": self.symbol,
            "side": side, "price": round(price, 4),
            "qty": qty, "pnl": round(pnl, 4),
        }})

    def status_dict(self):
        return {{
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "status": self.status,
            "position": self.position_qty,
            "cumulative_pnl": round(self.cumulative_pnl, 4),
            "mode": self.mode,
            "library_id": self.library_id,
        }}


# ── StrategyRunner (standard indicators) ───────────────────────────────────

class StrategyRunner(BaseRunner):

    def __init__(self, config, trade_ctx):
        super().__init__(config, trade_ctx)
        self.conditions = config.get("conditions", {{}})
        self.exit_rules = config.get("exit_rules", {{}})
        self.custom_script = config.get("custom_script", "")

        self.tp_pct = self.risk.get("tp_pct", self.risk.get("take_profit_pct", 5.0)) / 100
        self.sl_pct = self.risk.get("sl_pct", self.risk.get("stop_loss_pct", 2.0)) / 100
        self.trailing_pct = self.risk.get("trail_pct", self.risk.get("trailing_stop_pct", 0.0)) / 100
        self.qty_per_trade = self.risk.get("lots", self.risk.get("qty", 1))

        self.entry_price = 0.0
        self.peak_price = 0.0
        self.closes = deque(maxlen=500)
        self.highs = deque(maxlen=500)
        self.lows = deque(maxlen=500)

        self._custom_fn = None
        if self.mode == "script" and self.custom_script:
            self._compile_script()

        self.status = "running"
        self.log(f"Init: {{self.symbol}} tp={{self.tp_pct*100:.1f}}% sl={{self.sl_pct*100:.1f}}%")

    def _compile_script(self):
        try:
            src = ("class _R:\\n"
                   "    def ema(self,d,p):\\n"
                   "        d=list(d)\\n"
                   "        if len(d)<p:return None\\n"
                   "        m=2/(p+1);v=sum(d[:p])/p\\n"
                   "        for x in d[p:]:v=(x-v)*m+v\\n"
                   "        return v\\n"
                   "    def sma(self,d,p):d=list(d);return sum(d[-p:])/p if len(d)>=p else None\\n"
                   "    def rsi(self,d,p):\\n"
                   "        d=list(d)\\n"
                   "        if len(d)<p+1:return None\\n"
                   "        g=l=0.0\\n"
                   "        for i in range(-p,0):\\n"
                   "            diff=d[i]-d[i-1]\\n"
                   "            if diff>0:g+=diff\\n"
                   "            else:l-=diff\\n"
                   "        ag=g/p;al=l/p\\n"
                   "        return 100-(100/(1+ag/al)) if al else 100.0\\n"
                   "    def atr(self,h,l,c,p):\\n"
                   "        h,l,c=list(h),list(l),list(c)\\n"
                   "        if len(c)<p+1:return None\\n"
                   "        trs=[max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])) for i in range(1,len(c))]\\n"
                   "        return sum(trs[-p:])/p if len(trs)>=p else None\\n")
            src += "    " + self.custom_script.replace("\\n", "\\n    ") + "\\n"
            ns = {{}}
            exec(src, ns)
            self._custom_fn = ns["_R"]().get_signal
            self.log("Custom script compiled OK")
        except Exception as e:
            self.log(f"Script compile failed: {{e}}")

    def on_tick(self, price):
        # Check exits first
        exit_info = self._check_exit(price)
        if exit_info:
            return
        # Evaluate entry
        if self.position_qty != 0:
            return
        self.closes.append(price)
        self.highs.append(price + 0.01)
        self.lows.append(price - 0.01)

        sig = None
        if self.mode == "script" and self._custom_fn:
            try:
                sig = self._custom_fn(self.closes, self.highs, self.lows, deque(maxlen=500))
            except Exception:
                pass
        else:
            ctype = self.conditions.get("type", "price_cross")
            if ctype == "price_cross":
                thr = float(self.conditions.get("threshold", 0))
                d = self.conditions.get("direction", "above")
                if thr > 0:
                    if d == "above" and price > thr: sig = "buy"
                    elif d == "below" and price < thr: sig = "sell"
            elif ctype == "always_buy": sig = "buy"
            elif ctype == "always_sell": sig = "sell"
            elif ctype == "quick_test":
                if not hasattr(self, '_qt_done'):
                    self._qt_done = True
                    sig = "buy"
                    # Auto-sell after 30s
                    import threading as _thr
                    def _qt_sell():
                        import time as _t; _t.sleep(30)
                        if self.position_qty != 0:
                            self.log("[QUICK_TEST] Auto-sell after 30s")
                            self._close("sell", price)
                    _thr.Thread(target=_qt_sell, daemon=True).start()

        if sig in ("buy", "sell"):
            self._open(sig, price)

    def _open(self, side, price):
        self.position_qty = self.qty_per_trade if side == "buy" else -self.qty_per_trade
        self.entry_price = price
        self.peak_price = price
        self.log(f"OPEN {{side.upper()}} {{self.symbol}} @ ${{price:.4f}}")
        self.report_trade(side, price, abs(self.position_qty), 0.0)

    def _check_exit(self, price):
        if self.position_qty == 0:
            return None
        if self.position_qty > 0:
            self.peak_price = max(self.peak_price, price)
            pnl_pct = (price - self.entry_price) / self.entry_price
        else:
            self.peak_price = min(self.peak_price, price) if self.peak_price > 0 else price
            pnl_pct = (self.entry_price - price) / self.entry_price

        if self.tp_pct > 0 and pnl_pct >= self.tp_pct:
            return self._close(price, "TP")
        if self.sl_pct > 0 and pnl_pct <= -self.sl_pct:
            return self._close(price, "SL")
        if self.trailing_pct > 0:
            if self.position_qty > 0 and price <= self.peak_price * (1 - self.trailing_pct):
                return self._close(price, "TRAIL")
            if self.position_qty < 0 and price >= self.peak_price * (1 + self.trailing_pct):
                return self._close(price, "TRAIL")
        return None

    def _close(self, price, reason):
        qty = abs(self.position_qty)
        side = "sell" if self.position_qty > 0 else "buy"
        pnl = (price - self.entry_price) * qty if self.position_qty > 0 else (self.entry_price - price) * qty
        self.log(f"CLOSE {{side.upper()}} @ ${{price:.4f}} pnl=${{pnl:.2f}} ({{reason}})")
        self.report_trade(side, price, qty, round(pnl, 4))
        self.position_qty = 0
        self.entry_price = 0.0
        return True

    def on_fill(self, order_id, fill_price, fill_qty, side_str):
        pass  # Standard runner doesn't use LongPort order tracking

    def shutdown(self, last_price):
        if self.position_qty != 0 and last_price > 0:
            self._close(last_price, "SHUTDOWN")

    def status_dict(self):
        d = super().status_dict()
        d["entry_price"] = self.entry_price
        return d


# ── GridRunner (SYMMETRIC_GRID) ────────────────────────────────────────────

class GridRunner(BaseRunner):

    def __init__(self, config, trade_ctx):
        super().__init__(config, trade_ctx)
        params = config.get("conditions", {{}}).get("params", {{}})
        self.grid_levels_n = int(params.get("grid_levels", 4))
        self.grid_spacing_pct = float(params.get("grid_spacing_pct", 0.5))
        self.tp_pct = float(params.get("tp_pct", 0.5))
        self.sl_boundary_pct = float(params.get("sl_boundary_pct", 3.0))
        self.atr_multiplier = float(params.get("atr_multiplier", 0.75))
        self.lots = int(self.risk.get("lots", 1))
        self.lot_size = ARENA_LOT_SIZE.get(self.arena, 100)

        self.buy_levels = []
        self.cmp_reference = 0.0
        self.completed_cycles = 0
        self.initialized = False
        self._init_lock = threading.Lock()
        self.status = "waiting"
        self.log(f"Init: {{self.symbol}} levels={{self.grid_levels_n}} atr_mult={{self.atr_multiplier}} tp={{self.tp_pct}}%")

    def on_tick(self, price):
        if not self.initialized:
            with self._init_lock:
                if not self.initialized:
                    self._initialize(price)
            return
        self._check_sl(price)

    def _initialize(self, cmp):
        self.cmp_reference = cmp
        self.buy_levels = []
        spacing = cmp * self.grid_spacing_pct / 100.0
        order_shares = self.lots * self.lot_size

        self.log(f"Building grid around CMP=${{cmp:.4f}}")
        self.log(f"  Spacing: {{self.grid_spacing_pct}}% = ${{spacing:.4f}} | Shares: {{order_shares}}")

        for i in range(1, self.grid_levels_n + 1):
            entry = hk_tick_round(cmp - i * spacing, "buy", self.arena)
            tp = hk_tick_round(entry * (1 + self.tp_pct / 100), "sell", self.arena)
            self.buy_levels.append({{
                "level_id": f"BUY_{{i}}",
                "entry_price": entry, "tp_price": tp,
                "lots": order_shares, "filled": False,
                "order_id": "", "tp_order_id": "",
                "entry_fill_price": 0.0,
            }})
            self.log(f"  [BUY_{{i}}] entry=${{entry:.4f}} TP=${{tp:.4f}}")

        # Place buy-side orders
        self.log(f"Placing {{len(self.buy_levels)}} buy orders...")
        for lv in self.buy_levels:
            oid = self._submit_limit(OrderSide.Buy, lv["lots"], lv["entry_price"])
            lv["order_id"] = oid
            if oid:
                self.tracked_order_ids.add(oid)
            time.sleep(0.3)

        active = sum(1 for l in self.buy_levels if l["order_id"])
        self.log(f"Buy grid placed: {{active}}/{{len(self.buy_levels)}} active. Sells placed on fill.")
        self.initialized = True
        self.status = "running"

    def _submit_limit(self, side, qty, price):
        from longport.openapi import OrderType, TimeInForceType
        qty = max(self.lot_size, (qty // self.lot_size) * self.lot_size)
        price = hk_tick_round(price, "buy" if side == OrderSide.Buy else "sell", self.arena)
        s = "BUY" if side == OrderSide.Buy else "SELL"
        self.log(f"LIMIT {{s}} {{qty}} @ ${{price:.4f}}")
        try:
            resp = self.trade_ctx.submit_order(
                symbol=self.symbol, order_type=OrderType.LO, side=side,
                submitted_quantity=qty, time_in_force=TimeInForceType.Day,
                submitted_price=price,
            )
            oid = str(resp.order_id)
            self.log(f"Order placed: {{oid}}")
            return oid
        except Exception as e:
            self.log(f"Order error: {{e}}")
            return ""

    def on_fill(self, order_id, fill_price, fill_qty, side_str):
        for lv in self.buy_levels:
            # Entry BUY fill -> place TP SELL
            if lv["order_id"] == order_id and not lv["filled"]:
                lv["filled"] = True
                lv["entry_fill_price"] = fill_price
                self.position_qty += fill_qty
                self.tracked_order_ids.discard(order_id)
                self.report_trade("buy", fill_price, fill_qty, 0.0)

                tp_price = hk_tick_round(fill_price * (1 + self.tp_pct / 100), "sell", self.arena)
                tp_oid = self._submit_limit(OrderSide.Sell, fill_qty, tp_price)
                lv["tp_order_id"] = tp_oid
                if tp_oid:
                    self.tracked_order_ids.add(tp_oid)
                self.log(f"[{{lv['level_id']}}] Bought @ ${{fill_price:.4f}} -> TP SELL @ ${{tp_price:.4f}}")
                return

            # TP SELL fill -> PnL + replenish
            if lv["tp_order_id"] == order_id and lv["filled"]:
                entry_p = lv["entry_fill_price"]
                pnl = (fill_price - entry_p) * fill_qty
                self.position_qty -= fill_qty
                self.completed_cycles += 1
                self.tracked_order_ids.discard(order_id)
                self.report_trade("sell", fill_price, fill_qty, round(pnl, 4))
                self.log(f"[{{lv['level_id']}}] Sold @ ${{fill_price:.4f}} PnL=${{pnl:.2f}} cycle#{{self.completed_cycles}}")

                lv["filled"] = False
                lv["entry_fill_price"] = 0.0
                lv["tp_order_id"] = ""
                new_oid = self._submit_limit(OrderSide.Buy, lv["lots"], lv["entry_price"])
                lv["order_id"] = new_oid
                if new_oid:
                    self.tracked_order_ids.add(new_oid)
                self.log(f"[{{lv['level_id']}}] Buy replenished @ ${{lv['entry_price']:.4f}}")
                return

    def _check_sl(self, price):
        if not self.buy_levels:
            return
        lowest = self.buy_levels[-1]["entry_price"]
        sl_price = lowest * (1 - self.sl_boundary_pct / 100)
        if price < sl_price:
            self.log(f"SL BOUNDARY HIT: ${{price:.4f}} < ${{sl_price:.4f}}")
            self._flatten("SL boundary")

    def _flatten(self, reason):
        from longport.openapi import OrderType, TimeInForceType
        self.log(f"FLATTEN: {{reason}}")
        for lv in self.buy_levels:
            for key in ("order_id", "tp_order_id"):
                oid = lv.get(key, "")
                if oid:
                    try:
                        self.trade_ctx.cancel_order(oid)
                    except Exception:
                        pass
                    self.tracked_order_ids.discard(oid)
            lv["filled"] = False
        if self.position_qty > 0:
            try:
                self.trade_ctx.submit_order(
                    symbol=self.symbol, order_type=OrderType.MO,
                    side=OrderSide.Sell, submitted_quantity=self.position_qty,
                    time_in_force=TimeInForceType.Day,
                )
            except Exception as e:
                self.log(f"Flatten close error: {{e}}")
        self.status = "stopped"

    def shutdown(self, last_price):
        if self.initialized:
            self._flatten("Shutdown")

    def status_dict(self):
        d = super().status_dict()
        d["details"] = {{
            "cmp_reference": self.cmp_reference,
            "buy_orders_active": sum(1 for l in self.buy_levels if l["order_id"] and not l["filled"]),
            "positions_held": sum(1 for l in self.buy_levels if l["filled"]),
            "completed_cycles": self.completed_cycles,
        }}
        return d


# ── OrderManager ───────────────────────────────────────────────────────────

class OrderManager:
    def __init__(self, runners):
        self.runners = runners

    def on_order_changed(self, event):
        if event.status not in (OrderStatus.Filled, OrderStatus.PartialFilled):
            return
        oid = str(event.order_id)
        fill_price = float(event.executed_price or 0)
        fill_qty = int(event.executed_quantity or 0)
        side_str = "buy" if event.side == OrderSide.Buy else "sell"
        for runner in self.runners:
            if oid in runner.tracked_order_ids:
                runner.on_fill(oid, fill_price, fill_qty, side_str)
                return

    def on_quote(self, symbol, price):
        for runner in self.runners:
            if runner.symbol == symbol:
                runner.on_tick(price)


# ── Heartbeat ──────────────────────────────────────────────────────────────

def heartbeat_loop(runners):
    http = httpx.Client(timeout=10)
    while not _shutdown.is_set():
        _shutdown.wait(HEARTBEAT_INTERVAL)
        if _shutdown.is_set():
            break
        payload = {{
            "email": EMAIL, "vps_ip": "local", "bot_status": "running",
            "strategies": [r.status_dict() for r in runners],
        }}
        try:
            if CENTRAL_API_URL:
                http.post(f"{{CENTRAL_API_URL}}/api/heartbeat", json=payload, timeout=10)
        except Exception:
            pass


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    master_log.info("=" * 60)
    master_log.info("QuantX Master Bot — %s", EMAIL)
    master_log.info("Strategies: %d", len(STRATEGIES))
    master_log.info("=" * 60)

    # LongPort import
    try:
        from longport.openapi import (
            Config, QuoteContext, TradeContext, SubType, TopicType,
            OrderSide as _OS, OrderStatus as _OSt,
        )
        global OrderSide, OrderStatus
        OrderSide = _OS
        OrderStatus = _OSt
    except ImportError:
        master_log.error("longport package not installed")
        return

    # Connect
    trade_ctx = None
    quote_ctx = None
    try:
        cfg = Config(app_key=APP_KEY, app_secret=APP_SECRET, access_token=ACCESS_TOKEN)
        trade_ctx = TradeContext(cfg)
        quote_ctx = QuoteContext(cfg)
        master_log.info("LongPort connected")
    except Exception as e:
        master_log.warning("LongPort connect failed (simulation): %s", e)

    # Create runners
    runners = []
    for s in STRATEGIES:
        indicator = s.get("library_id", "") or s.get("conditions", {{}}).get("type", "")
        if indicator == "SYMMETRIC_GRID":
            runners.append(GridRunner(s, trade_ctx))
        else:
            runners.append(StrategyRunner(s, trade_ctx))

    if not runners:
        master_log.error("No strategies. Exiting.")
        return

    symbols = list(set(r.symbol for r in runners))
    master_log.info("Symbols: %s", symbols)
    master_log.info("Runners: %s", [f"{{r.strategy_id}}({{type(r).__name__}})" for r in runners])

    # Wire up order fill routing
    order_mgr = OrderManager(runners)

    if trade_ctx:
        trade_ctx.set_on_order_changed(order_mgr.on_order_changed)
        trade_ctx.subscribe([TopicType.Private])

    # Subscribe quotes for all symbols
    if quote_ctx:
        def _on_quote(symbol, event):
            price = float(event.last_done or 0)
            if price > 0:
                order_mgr.on_quote(symbol, price)

        quote_ctx.set_on_quote(_on_quote)
        try:
            quote_ctx.subscribe(symbols, [SubType.Quote])
            master_log.info("Subscribed to quotes: %s", symbols)
        except Exception as e:
            master_log.warning("Quote subscribe failed: %s", e)

        # Fetch initial prices to kick off grid runners
        try:
            quotes = quote_ctx.quote(symbols)
            for q in quotes:
                sym = q.symbol
                price = float(q.last_done)
                if price > 0:
                    master_log.info("Initial quote %s: $%.4f", sym, price)
                    order_mgr.on_quote(sym, price)
        except Exception as e:
            master_log.warning("Initial quote fetch failed: %s — grid runners will init on first tick", e)

    # Heartbeat
    hb = threading.Thread(target=heartbeat_loop, args=(runners,), daemon=True)
    hb.start()

    master_log.info("Bot is LIVE. Monitoring quotes and fills...")

    # Main loop
    try:
        while not _shutdown.is_set():
            _shutdown.wait(1)
    except KeyboardInterrupt:
        pass

    master_log.info("Shutting down...")
    last_prices = {{}}
    if quote_ctx:
        try:
            for q in quote_ctx.quote(symbols):
                last_prices[q.symbol] = float(q.last_done)
        except Exception:
            pass
    for r in runners:
        r.shutdown(last_prices.get(r.symbol, 0))
    master_log.info("Bot stopped.")


if __name__ == "__main__":
    main()
'''
