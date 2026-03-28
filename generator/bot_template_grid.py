"""QuantX Grid Bot template — ATR-based dynamic grid with daily reset."""

GRID_BOT_TEMPLATE = '''#!/usr/bin/env python3
"""
QuantX Grid Bot — {STRATEGY_ID}
ATR-based Symmetric Grid on {SYMBOL} (buy-side only)
Daily reset: cancels at close, rebuilds at open with fresh ATR.
"""

import json
import math
import os
import sqlite3
import threading
import time
import urllib.request
from datetime import datetime, timezone, timedelta

from longport.openapi import (
    Config, OrderSide, OrderStatus, OrderType,
    QuoteContext, SubType, TimeInForceType,
    TopicType, TradeContext,
)

# ── Credentials ──────────────────────────────────
APP_KEY      = "{APP_KEY}"
APP_SECRET   = "{APP_SECRET}"
ACCESS_TOKEN = "{ACCESS_TOKEN}"

# ── Config ────────────────────────────────────────
STUDENT_EMAIL = "{STUDENT_EMAIL}"
STRATEGY_ID   = "{STRATEGY_ID}"
SYMBOL        = "{SYMBOL}"
LOT_SIZE      = {LOT_SIZE}
ARENA         = "{ARENA}"
API_URL       = "{API_URL}"
LOTS          = {LOTS}

# ── Grid Parameters ───────────────────────────────
GRID_LEVELS       = {GRID_LEVELS}
GRID_SPACING_PCT  = {GRID_SPACING_PCT}
TP_PCT            = {TP_PCT}
SL_BOUNDARY_PCT   = {SL_BOUNDARY_PCT}
ATR_MULTIPLIER    = {ATR_MULTIPLIER}

SGT = timezone(timedelta(hours=8))

# ── Global State ──────────────────────────────────
cmp_reference    = 0.0
grid_levels_data = []
cumulative_pnl   = 0.0
completed_cycles = 0
last_price       = 0.0
stop_event       = threading.Event()
trade_ctx_ref    = None
quote_ctx_ref    = None
position_qty     = 0


def log(msg):
    ts = datetime.now(SGT).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{{ts}}][{{STRATEGY_ID}}] {{msg}}", flush=True)


# ── HK Tick Size ──────────────────────────────────

def hk_tick_round(price, side="buy"):
    if ARENA != "HK":
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


# ── Market Hours ──────────────────────────────────

def is_market_open():
    now = datetime.now(SGT)
    wd = now.weekday()
    h, m = now.hour, now.minute
    if ARENA in ("HK", "HK_ETF"):
        if wd >= 5: return False
        return (h > 9 or (h == 9 and m >= 30)) and h < 16
    else:
        if wd == 5:
            return h < 4
        if wd == 6:
            return h > 21 or (h == 21 and m >= 30)
        return (h > 21 or (h == 21 and m >= 30)) or h < 4

def seconds_until_open():
    now = datetime.now(SGT)
    wd = now.weekday()
    if ARENA in ("HK", "HK_ETF"):
        c = now.replace(hour=9, minute=30, second=0, microsecond=0)
        if now >= c or wd >= 5:
            days = 1
            if wd == 4: days = 3
            if wd == 5: days = 2
            c += timedelta(days=days)
        return max(0, (c - now).total_seconds())
    else:
        c = now.replace(hour=21, minute=30, second=0, microsecond=0)
        if now >= c: c += timedelta(days=1)
        while c.weekday() == 5: c += timedelta(days=1)
        return max(0, (c - now).total_seconds())

def seconds_until_close():
    now = datetime.now(SGT)
    if ARENA in ("HK", "HK_ETF"):
        c = now.replace(hour=16, minute=0, second=0, microsecond=0)
        return max(0, (c - now).total_seconds()) if now < c else 0
    else:
        c = now.replace(hour=4, minute=0, second=0, microsecond=0)
        if now >= c: c += timedelta(days=1)
        return (c - now).total_seconds()


# ── ATR Calculation ───────────────────────────────

def calculate_atr_spacing(quote_ctx):
    try:
        from longport.openapi import Period, AdjustType
        bars = quote_ctx.history_candlesticks_by_offset(SYMBOL, Period.Day, AdjustType.NoAdjust, 20)
        if not bars or len(bars) < 15:
            log(f"ATR: insufficient bars ({{len(bars) if bars else 0}})")
            return None
        closes = [float(b.close) for b in bars]
        highs = [float(b.high) for b in bars]
        lows = [float(b.low) for b in bars]
        trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(bars))]
        atr14 = sum(trs[-14:]) / min(14, len(trs))
        spacing = atr14 * ATR_MULTIPLIER
        cmp = closes[-1]
        mn, mx = cmp * 0.001, cmp * 0.03
        spacing = max(mn, min(mx, spacing))
        spacing = hk_tick_round(spacing, "buy") if ARENA in ("HK","HK_ETF") else round(spacing, 2)
        log(f"ATR(14)=${{atr14:.4f}} x {{ATR_MULTIPLIER}} = spacing=${{spacing:.4f}} ({{spacing/cmp*100:.3f}}%)")
        return spacing
    except Exception as e:
        log(f"ATR failed: {{e}}")
        return None


# ── Helpers ───────────────────────────────────────

def post_trade(side, price, qty, pnl):
    global cumulative_pnl
    cumulative_pnl += pnl
    body = json.dumps({{"email": STUDENT_EMAIL, "strategy_id": STRATEGY_ID, "symbol": SYMBOL,
        "side": side, "price": round(price, 4), "qty": qty, "pnl": round(pnl, 4)}}).encode()
    req = urllib.request.Request(f"{{API_URL}}/api/trade", data=body,
        headers={{"Content-Type": "application/json"}}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            r = json.loads(resp.read())
            log(f"Trade: {{side.upper()}} {{qty}} @ ${{price:.4f}} PnL=${{pnl:.2f}} Cum=${{r.get('cumulative_pnl',0):.2f}}")
    except Exception as e:
        log(f"Trade post failed: {{e}}")

def post_heartbeat():
    buy_active = sum(1 for l in grid_levels_data if l.get("order_id") and not l["filled"])
    held = sum(1 for l in grid_levels_data if l["filled"])
    body = json.dumps({{"email": STUDENT_EMAIL, "vps_ip": "local", "bot_status": "running",
        "strategies": [{{"strategy_id": STRATEGY_ID, "indicator": "SYMMETRIC_GRID",
            "symbol": SYMBOL, "status": "running", "position": position_qty,
            "cumulative_pnl": round(cumulative_pnl, 4), "mode": "library",
            "details": {{"cmp_reference": cmp_reference, "buy_orders_active": buy_active,
                "positions_held": held, "completed_cycles": completed_cycles,
                "last_price": last_price, "market_open": is_market_open()}}}}]}}).encode()
    req = urllib.request.Request(f"{{API_URL}}/api/heartbeat", data=body,
        headers={{"Content-Type": "application/json"}}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass

def submit_limit_order(trade_ctx, side, qty, price):
    qty = max(LOT_SIZE, (qty // LOT_SIZE) * LOT_SIZE)
    price = hk_tick_round(price, "buy" if side == OrderSide.Buy else "sell")
    s = "BUY" if side == OrderSide.Buy else "SELL"
    log(f"LIMIT {{s}} {{qty}} @ ${{price:.4f}}")
    try:
        resp = trade_ctx.submit_order(symbol=SYMBOL, order_type=OrderType.LO, side=side,
            submitted_quantity=qty, time_in_force=TimeInForceType.Day, submitted_price=price)
        log(f"Order placed: {{resp.order_id}}")
        return str(resp.order_id)
    except Exception as e:
        log(f"Order error: {{e}}")
        return ""


# ── Grid Construction ─────────────────────────────

def build_grid(cmp, atr_spacing=None):
    global grid_levels_data, cmp_reference
    cmp_reference = cmp
    grid_levels_data = []
    spacing = atr_spacing if atr_spacing and atr_spacing > 0 else round(cmp * GRID_SPACING_PCT / 100.0, 4)
    tp_dist = spacing
    shares = LOTS * LOT_SIZE
    log(f"Grid CMP=${{cmp:.4f}} spacing=${{spacing:.4f}} tp=${{tp_dist:.4f}} levels={{GRID_LEVELS}} shares={{shares}}")
    for i in range(1, GRID_LEVELS + 1):
        entry = hk_tick_round(cmp - i * spacing, "buy")
        tp = hk_tick_round(entry + tp_dist, "sell")
        grid_levels_data.append({{"level_id": f"BUY_{{i}}", "side": "buy", "entry_price": entry,
            "tp_price": tp, "lots": shares, "filled": False, "active": False,
            "order_id": "", "tp_order_id": "", "entry_fill_price": 0.0}})
        log(f"  [BUY_{{i}}] entry=${{entry:.4f}} TP=${{tp:.4f}}")

def place_all_grid_orders(trade_ctx):
    log(f"Placing {{len(grid_levels_data)}} buy orders...")
    for lv in grid_levels_data:
        oid = submit_limit_order(trade_ctx, OrderSide.Buy, lv["lots"], lv["entry_price"])
        lv["order_id"] = oid
        lv["active"] = bool(oid)
        time.sleep(0.3)
    active = sum(1 for l in grid_levels_data if l["active"])
    log(f"Buy grid placed: {{active}}/{{len(grid_levels_data)}} active")

def cancel_all_grid_orders(trade_ctx):
    cancelled = 0
    for lv in grid_levels_data:
        for k in ("order_id", "tp_order_id"):
            oid = lv.get(k, "")
            if oid:
                try:
                    trade_ctx.cancel_order(oid)
                    cancelled += 1
                except Exception:
                    pass
                lv[k] = ""
        lv["active"] = False
        lv["filled"] = False
    log(f"Cancelled {{cancelled}} orders")

def close_all_positions(trade_ctx):
    global position_qty
    if position_qty == 0:
        log("No positions to close")
        return
    side = OrderSide.Sell if position_qty > 0 else OrderSide.Buy
    qty = abs(position_qty)
    log(f"Closing {{qty}} shares")
    try:
        trade_ctx.submit_order(symbol=SYMBOL, order_type=OrderType.MO, side=side,
            submitted_quantity=qty, time_in_force=TimeInForceType.Day)
    except Exception as e:
        log(f"Close error: {{e}}")


# ── Fill Handler ──────────────────────────────────

def on_order_changed(event):
    global position_qty, completed_cycles
    if event.symbol != SYMBOL or event.status not in (OrderStatus.Filled, OrderStatus.PartialFilled):
        return
    fp = float(event.executed_price or 0)
    fq = int(event.executed_quantity or 0)
    oid = str(event.order_id)
    side = "buy" if event.side == OrderSide.Buy else "sell"
    log(f"FILL: {{side.upper()}} {{fq}} @ ${{fp:.4f}} oid={{oid}}")

    for lv in grid_levels_data:
        if lv["order_id"] == oid and not lv["filled"]:
            lv["filled"] = True
            lv["entry_fill_price"] = fp
            position_qty += fq
            post_trade("buy", fp, fq, 0.0)
            tp_p = hk_tick_round(fp * (1 + TP_PCT / 100), "sell")
            tp_oid = submit_limit_order(trade_ctx_ref, OrderSide.Sell, fq, tp_p)
            lv["tp_order_id"] = tp_oid
            log(f"[{{lv['level_id']}}] Bought @ ${{fp:.4f}} -> TP SELL @ ${{tp_p:.4f}}")
            return
        if lv["tp_order_id"] == oid and lv["filled"]:
            pnl = (fp - lv["entry_fill_price"]) * fq
            position_qty -= fq
            completed_cycles += 1
            post_trade("sell", fp, fq, round(pnl, 4))
            log(f"[{{lv['level_id']}}] Sold @ ${{fp:.4f}} PnL=${{pnl:.2f}} cycle#{{completed_cycles}}")
            lv["filled"] = False
            lv["entry_fill_price"] = 0.0
            lv["tp_order_id"] = ""
            new_oid = submit_limit_order(trade_ctx_ref, OrderSide.Buy, lv["lots"], lv["entry_price"])
            lv["order_id"] = new_oid
            lv["active"] = bool(new_oid)
            log(f"[{{lv['level_id']}}] Buy replenished @ ${{lv['entry_price']:.4f}}")
            return


# ── SL + Quote ────────────────────────────────────

def check_sl(price):
    if not grid_levels_data: return
    lowest = grid_levels_data[-1]["entry_price"]
    sl = lowest * (1 - SL_BOUNDARY_PCT / 100)
    if price < sl:
        log(f"SL HIT: ${{price:.4f}} < ${{sl:.4f}}")
        cancel_all_grid_orders(trade_ctx_ref)
        close_all_positions(trade_ctx_ref)

def on_quote(symbol, event):
    global last_price
    p = float(event.last_done or 0)
    if p > 0 and symbol == SYMBOL:
        last_price = p
        if trade_ctx_ref: check_sl(p)


# ── EOD Manager ───────────────────────────────────

def eod_manager_thread(trade_ctx):
    log(f"EOD manager started ({{ARENA}})")
    while not stop_event.is_set():
        try:
            if not is_market_open():
                ws = seconds_until_open()
                log(f"Market CLOSED. Next open in {{int(ws//3600)}}h {{int((ws%3600)//60)}}m")
                waited = 0
                while waited < ws and not stop_event.is_set():
                    stop_event.wait(min(60, ws - waited))
                    waited += 60
                if stop_event.is_set(): break
                log("MARKET OPENING - rebuilding grid")
                restart_grid(trade_ctx)
            else:
                stc = seconds_until_close()
                if stc <= 300:
                    log(f"MARKET CLOSING in {{int(stc)}}s - EOD cleanup")
                    cancel_all_grid_orders(trade_ctx)
                    close_all_positions(trade_ctx)
                    ws = seconds_until_open()
                    log(f"EOD done. Next open in {{int(ws//3600)}}h {{int((ws%3600)//60)}}m")
                    waited = 0
                    while waited < ws and not stop_event.is_set():
                        stop_event.wait(min(60, ws - waited))
                        waited += 60
                    if not stop_event.is_set():
                        restart_grid(trade_ctx)
                else:
                    stop_event.wait(60)
        except Exception as e:
            log(f"EOD error: {{e}}")
            stop_event.wait(60)

def restart_grid(trade_ctx):
    log("Fetching fresh quote...")
    for attempt in range(5):
        try:
            quotes = quote_ctx_ref.quote([SYMBOL])
            if quotes and float(quotes[0].last_done) > 0:
                cmp = float(quotes[0].last_done)
                log(f"Fresh CMP: ${{cmp:.4f}}")
                atr_sp = calculate_atr_spacing(quote_ctx_ref)
                build_grid(cmp, atr_sp)
                place_all_grid_orders(trade_ctx)
                log("Grid rebuilt")
                return
        except Exception as e:
            log(f"Quote attempt {{attempt+1}}/5: {{e}}")
            stop_event.wait(10)
    log("WARNING: could not rebuild grid")


# ── Heartbeat Thread ──────────────────────────────

def heartbeat_thread():
    while not stop_event.is_set():
        time.sleep(60)
        if stop_event.is_set(): break
        post_heartbeat()


# ── Main ──────────────────────────────────────────

def main():
    global trade_ctx_ref, quote_ctx_ref

    log("=" * 60)
    log(f"QuantX Grid Bot (ATR-based, daily reset)")
    log(f"Symbol: {{SYMBOL}} | Arena: {{ARENA}} | Lots: {{LOTS}}")
    log(f"Levels: {{GRID_LEVELS}} | ATR mult: {{ATR_MULTIPLIER}} | SL: {{SL_BOUNDARY_PCT}}%")
    log("=" * 60)

    config = Config(app_key=APP_KEY, app_secret=APP_SECRET, access_token=ACCESS_TOKEN)
    trade_ctx = TradeContext(config)
    trade_ctx.set_on_order_changed(on_order_changed)
    trade_ctx_ref = trade_ctx
    trade_ctx.subscribe([TopicType.Private])
    log("TradeContext connected")

    quote_ctx = QuoteContext(config)
    quote_ctx_ref = quote_ctx
    log("QuoteContext connected")

    now_sgt = datetime.now(SGT).strftime("%H:%M SGT %A")
    mkt = "OPEN" if is_market_open() else "CLOSED"
    log(f"Time: {{now_sgt}} | Market: {{mkt}}")

    # Start EOD manager (handles market hours + daily reset)
    eod_t = threading.Thread(target=eod_manager_thread, args=(trade_ctx,), daemon=True)
    eod_t.start()
    hb_t = threading.Thread(target=heartbeat_thread, daemon=True)
    hb_t.start()

    if is_market_open():
        log("Market OPEN - starting grid now")
        for attempt in range(5):
            try:
                quotes = quote_ctx.quote([SYMBOL])
                if quotes and float(quotes[0].last_done) > 0:
                    cmp = float(quotes[0].last_done)
                    log(f"CMP: ${{cmp:.4f}}")
                    break
            except Exception as e:
                log(f"Quote {{attempt+1}}/5: {{e}}")
                time.sleep(5)
        else:
            log("ERROR: no quote. EOD manager will retry at next open.")
            cmp = None

        if cmp:
            atr_sp = calculate_atr_spacing(quote_ctx)
            build_grid(cmp, atr_sp)
            place_all_grid_orders(trade_ctx)
            quote_ctx.set_on_quote(on_quote)
            quote_ctx.subscribe([SYMBOL], [SubType.Quote])
            log("Grid LIVE. Daily reset enabled.")
    else:
        ws = seconds_until_open()
        log(f"Market CLOSED. Auto-start in {{int(ws//3600)}}h {{int((ws%3600)//60)}}m")

    try:
        while not stop_event.is_set():
            stop_event.wait(1)
    except KeyboardInterrupt:
        log("Shutdown requested")
    finally:
        stop_event.set()
        cancel_all_grid_orders(trade_ctx)
        close_all_positions(trade_ctx)
        log("Grid bot stopped.")


if __name__ == "__main__":
    main()
'''
