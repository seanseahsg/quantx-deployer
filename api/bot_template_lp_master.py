"""QuantX -- LongPort master bot template.

One process, one QuoteContext, one TradeContext, multiple strategies.
Uses __PLACEHOLDER__ substitution (same pattern as IBKR prod template).
"""

LP_MASTER_TEMPLATE = r'''#!/usr/bin/env python3
"""
================================================================================
QuantX LongPort Master Bot
Email     : __EMAIL__
Strategies: __STRATEGY_COUNT__
Generated : by QuantX Deployer
================================================================================

Architecture:
  ONE QuoteContext  -- shared by all strategies (1 WebSocket)
  ONE TradeContext  -- shared by all strategies (1 WebSocket)
  Total connections: 2 (well within LongPort's 10-connection limit)
"""
import os, sys, json, math, csv, time, signal as _signal, logging, threading, decimal
from collections import deque
from datetime import datetime, timezone, timedelta

try:
    import requests
except ImportError:
    requests = None
try:
    import httpx as _httpx
except ImportError:
    _httpx = None

EMAIL          = '__EMAIL__'
CENTRAL_API_URL = '__CENTRAL_API_URL__'
LOCAL_API_URL  = 'http://127.0.0.1:8080'
APP_KEY        = '__APP_KEY__'
APP_SECRET     = '__APP_SECRET__'
ACCESS_TOKEN   = '__ACCESS_TOKEN__'

DRY_RUN    = __DRY_RUN__   # True = simulate orders, no real trades

LOG_DIR    = '__LOG_DIR__'
TRADES_DIR = '__TRADES_DIR__'
STATE_DIR  = '__STATE_DIR__'
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TRADES_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

SGT = timezone(timedelta(hours=8))
HEARTBEAT_INTERVAL = 60

# Strategies list -- injected at generation time
STRATEGIES = __STRATEGIES_LIST__

# ── Logging ────────────────────────────────────────────────────────────────
# Clear any existing handlers to prevent duplicates if module is reloaded
_root_logger = logging.getLogger()
_root_logger.handlers.clear()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, '__LOG_NAME__'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger('quantx-lp-master')
# Prevent log propagation to root which would cause double logging
logger.propagate = False

_shutdown = threading.Event()

def _handle_signal(signum, frame):
    logger.warning('Signal %s -- shutting down', signum)
    _shutdown.set()

_signal.signal(_signal.SIGTERM, _handle_signal)
_signal.signal(_signal.SIGINT, _handle_signal)


# ── Indicator functions (same as IBKR prod) ───────────────────────────────

def calc_ema(vals, period):
    r = [None]*len(vals); k = 2.0/(period+1)
    for i in range(len(vals)):
        if i < period-1: continue
        if r[i-1] is None: r[i] = sum(vals[max(0,i-period+1):i+1])/period
        else: r[i] = vals[i]*k + r[i-1]*(1-k)
    return r

def calc_sma(vals, period):
    r = [None]*len(vals)
    for i in range(period-1, len(vals)): r[i] = sum(vals[i-period+1:i+1])/period
    return r

def calc_rsi(vals, period=14):
    r = [None]*len(vals)
    for i in range(period, len(vals)):
        g = [max(0, vals[j]-vals[j-1]) for j in range(i-period+1, i+1)]
        l = [max(0, vals[j-1]-vals[j]) for j in range(i-period+1, i+1)]
        ag, al = sum(g)/period, sum(l)/period
        r[i] = 100.0 if al == 0 else 100 - 100/(1 + ag/al)
    return r

def calc_macd(vals, fast=12, slow=26, sig=9):
    ef, es = calc_ema(vals, fast), calc_ema(vals, slow)
    ml = [None if a is None or b is None else a-b for a,b in zip(ef, es)]
    sl = [None]*len(ml); k = 2.0/(sig+1); sv = None
    for i, v in enumerate(ml):
        if v is None: continue
        sv = v if sv is None else v*k + sv*(1-k)
        sl[i] = sv
    hist = [None if a is None or b is None else a-b for a,b in zip(ml, sl)]
    return ml, sl, hist

def calc_bbands(vals, period=20, std=2.0):
    mid = calc_sma(vals, period); u=[None]*len(vals); l=[None]*len(vals)
    for i in range(period-1, len(vals)):
        if mid[i] is None: continue
        sd = math.sqrt(sum((vals[i-j]-mid[i])**2 for j in range(period))/period)
        u[i] = mid[i]+std*sd; l[i] = mid[i]-std*sd
    return u, mid, l

def calc_atr(highs, lows, closes, period=14):
    trs = [None]; r = [None]*len(closes)
    for i in range(1, len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    for i in range(period, len(trs)):
        if None in trs[i-period+1:i+1]: continue
        r[i] = sum(trs[i-period+1:i+1])/period
    return r

def calc_roc(vals, period=10):
    r = [None]*len(vals)
    for i in range(period, len(vals)):
        if vals[i-period] and vals[i-period] != 0:
            r[i] = (vals[i]-vals[i-period])/vals[i-period]*100
    return r

def calc_zscore(vals, period=20):
    r = [None]*len(vals)
    for i in range(period, len(vals)):
        w = vals[i-period+1:i+1]; m = sum(w)/period
        s = (sum((x-m)**2 for x in w)/period)**0.5
        r[i] = (vals[i]-m)/s if s > 0 else 0
    return r

def calc_obv(closes, volumes):
    r = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i-1]: r.append(r[-1]+volumes[i])
        elif closes[i] < closes[i-1]: r.append(r[-1]-volumes[i])
        else: r.append(r[-1])
    return r

def calc_vwap(highs, lows, closes, volumes):
    r = [None]*len(closes); ctv = cv = 0.0
    for i in range(len(closes)):
        tp = (highs[i]+lows[i]+closes[i])/3; ctv += tp*volumes[i]; cv += volumes[i]
        r[i] = ctv/cv if cv > 0 else closes[i]
    return r

def calc_stoch(highs, lows, closes, k_period=14, d_period=3):
    kl = [None]*len(closes)
    for i in range(k_period-1, len(closes)):
        h = max(highs[i-k_period+1:i+1]); l = min(lows[i-k_period+1:i+1])
        kl[i] = (closes[i]-l)/(h-l)*100 if h != l else 50
    dl = [None]*len(closes)
    for i in range(k_period+d_period-2, len(closes)):
        vs = [kl[j] for j in range(i-d_period+1,i+1) if kl[j] is not None]
        if len(vs) == d_period: dl[i] = sum(vs)/d_period
    return kl, dl

def calc_williams_r(highs, lows, closes, period=14):
    r = [None]*len(closes)
    for i in range(period-1, len(closes)):
        h = max(highs[i-period+1:i+1]); l = min(lows[i-period+1:i+1])
        r[i] = (h-closes[i])/(h-l)*-100 if h != l else -50
    return r

def calc_donchian(highs, lows, period=20):
    u = [None]*len(highs); l = [None]*len(lows)
    for i in range(period-1, len(highs)):
        u[i] = max(highs[i-period+1:i+1]); l[i] = min(lows[i-period+1:i+1])
    return u, l

def calc_wma(vals, period):
    r = [None]*len(vals); wts = list(range(1,period+1)); ws = sum(wts)
    for i in range(period-1, len(vals)):
        r[i] = sum(vals[i-period+1+j]*wts[j] for j in range(period))/ws
    return r

def calc_hma(vals, period):
    h = period//2; sq = max(int(period**0.5),1)
    wh = calc_wma(vals, h); wf = calc_wma(vals, period)
    d = [2*wh[i]-wf[i] if wh[i] is not None and wf[i] is not None else 0 for i in range(len(vals))]
    return calc_wma(d, sq)

def calc_supertrend(highs, lows, closes, period=10, mult=3.0):
    atr_v = calc_atr(highs, lows, closes, period)
    di = [1]*len(closes); st = [None]*len(closes)
    ub = [None]*len(closes); lb = [None]*len(closes)
    for i in range(period, len(closes)):
        if atr_v[i] is None: continue
        mid = (highs[i]+lows[i])/2
        ub[i] = mid+mult*atr_v[i]; lb[i] = mid-mult*atr_v[i]
        if i == period: di[i]=1; st[i]=lb[i]; continue
        if di[i-1] == 1:
            if lb[i-1]: lb[i] = max(lb[i], lb[i-1])
            if closes[i] < lb[i]: di[i]=-1; st[i]=ub[i]
            else: di[i]=1; st[i]=lb[i]
        else:
            if ub[i-1]: ub[i] = min(ub[i], ub[i-1])
            if closes[i] > ub[i]: di[i]=1; st[i]=lb[i]
            else: di[i]=-1; st[i]=ub[i]
    return st, di


# ── Per-strategy signal functions (GENERATED) ─────────────────────────────
__SIGNAL_FUNCTIONS__


# ── HK tick rounding ──────────────────────────────────────────────────────

def _hk_tick(p):
    """Return the correct HK tick size for a given price."""
    if p < 0.25: return 0.001
    elif p < 0.5: return 0.005
    elif p < 10: return 0.010
    elif p < 20: return 0.020
    elif p < 100: return 0.050
    elif p < 200: return 0.100
    elif p < 500: return 0.200
    elif p < 1000: return 0.500
    elif p < 2000: return 1.000
    elif p < 5000: return 2.000
    else: return 5.000

def hk_tick_round(p):
    """Round DOWN to nearest HK tick (for BUY limit orders)."""
    tick = _hk_tick(p)
    return round(math.floor(p / tick) * tick, 4)

def hk_tick_round_sell(p):
    """Round UP to nearest HK tick (for SELL limit orders)."""
    tick = _hk_tick(p)
    return round(math.ceil(p / tick) * tick, 4)


def calc_hk_fees(price, qty, side):
    """
    LongBridge HK stock fees. side: 'buy' or 'sell'.
    HK abolished sell-side stamp duty Oct 2023 — buys only.
    """
    value      = price * qty
    brokerage  = max(value * 0.0003, 3.0)        # 0.03% min HKD 3
    platform   = min(15.0, value * 0.0003)        # HKD 15 capped at 0.03%
    sfc_levy   = value * 0.000027                 # SFC levy 0.0027%
    hkex_fee   = value * 0.00005                  # HKEX trading fee 0.005%
    ccass      = min(value * 0.00002, 100.0)      # CCASS 0.002% max HKD100
    stamp      = value * 0.001 if side == 'buy' else 0.0  # stamp duty buy only
    return round(brokerage + platform + sfc_levy + hkex_fee + ccass + stamp, 2)

CSV_FIELDS = ['execId','order_id','datetime','bar_date','strategy','symbol',
              'side','quantity','price','commission','pnl','signal',
              'indicator_values','bar_close','orderRef']

def ensure_csv(trades_file):
    if not os.path.exists(trades_file):
        with open(trades_file, 'w', newline='') as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

def log_trade(strategy_id, symbol, side, qty, price, pnl=0.0, signal='',
              bar_date='', indicator_values='', bar_close=0.0, order_id=''):
    trades_file = os.path.join(TRADES_DIR, f'trades_{strategy_id}_all.csv')
    ensure_csv(trades_file)
    eid = f'{strategy_id}_{datetime.now(timezone.utc):%Y%m%d%H%M%S%f}'
    with open(trades_file, 'a', newline='') as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow({
            'execId': eid, 'order_id': order_id or eid,
            'datetime': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'bar_date': bar_date, 'strategy': strategy_id,
            'symbol': symbol, 'side': side,
            'quantity': qty, 'price': round(price, 6),
            'commission': 0, 'pnl': round(pnl, 2), 'signal': signal,
            'indicator_values': indicator_values,
            'bar_close': round(bar_close, 6), 'orderRef': strategy_id})
    logger.info(f'[TRADE][{strategy_id}] {side} {qty} {symbol} @ {price:.4f} | pnl=${pnl:+.2f} | signal={signal}')
    _payload = {'email': EMAIL, 'strategy_id': strategy_id, 'symbol': symbol,
                'side': side.lower(), 'price': float(price), 'qty': float(qty), 'pnl': float(pnl)}
    def _post(url, payload, timeout):
        try:
            if requests:
                requests.post(url, json=payload, timeout=timeout)
            elif _httpx:
                _httpx.post(url, json=payload, timeout=timeout)
        except Exception:
            pass
    _post(f'{LOCAL_API_URL}/api/trade', _payload, 2)
    if CENTRAL_API_URL:
        _post(f'{CENTRAL_API_URL}/api/trade', _payload, 3)


# ── Strategy state ─────────────────────────────────────────────────────────

class StrategyState:
    """Per-strategy position and data tracking."""
    def __init__(self, config, compute_fn):
        self.sid = config['strategy_id']
        self.symbol = config['symbol']
        self.arena = config.get('arena', 'HK')
        self.timeframe = config.get('timeframe', '1day')
        self.compute_fn = compute_fn
        self.risk = config.get('risk', {})
        self.lot_size = int(self.risk.get('lots', 100))
        self.tp_pct = float(self.risk.get('tp_pct', 5)) / 100
        self.sl_pct = float(self.risk.get('sl_pct', 2)) / 100
        if self.tp_pct > 1: self.tp_pct /= 100
        if self.sl_pct > 1: self.sl_pct /= 100
        self.has_short = bool(config.get('has_short', False))

        # Position state: first check injected initial state, then state file
        self.current_position = int(config.get('initial_position', 0))
        self.entry_price = float(config.get('initial_entry_price', 0.0))
        self.data_buffer = deque(maxlen=500)

        self.pos_file = os.path.join(STATE_DIR, f'pos_{self.sid}.json')
        self._load_state()

    def _load_state(self):
        """Load from state file. Only overrides if file has a non-zero position
        and we don't already have an injected position."""
        try:
            if os.path.exists(self.pos_file):
                s = json.load(open(self.pos_file))
                file_pos = int(s.get('current_position', 0))
                file_entry = float(s.get('entry_price', 0.0))
                # State file takes precedence (it's the most recent truth)
                if file_pos != 0:
                    self.current_position = file_pos
                    self.entry_price = file_entry
                    logger.info(f'[{self.sid}] Restored from file: pos={self.current_position} entry={self.entry_price:.4f}')
                elif self.current_position != 0:
                    logger.info(f'[{self.sid}] Using injected state: pos={self.current_position} entry={self.entry_price:.4f}')
        except Exception as e:
            logger.warning(f'[{self.sid}] State load failed: {e}')
            if self.current_position != 0:
                logger.info(f'[{self.sid}] Falling back to injected state: pos={self.current_position}')

    def save_state(self):
        try:
            json.dump({'current_position': self.current_position,
                       'entry_price': self.entry_price}, open(self.pos_file, 'w'))
        except Exception as e:
            logger.warning(f'[{self.sid}] State save failed: {e}')


# ── Grid state ─────────────────────────────────────────────────────────────

class GridState:
    """
    Event-driven grid bot state.
    Initializes a buy ladder below CMP on first price tick.
    On each buy fill → places TP sell above fill price.
    On each sell fill → replenishes the buy level.
    On SL boundary breach → flattens all positions.
    Works in both LIVE and DRY_RUN modes.
    """
    def __init__(self, config, trade_ctx, quote_ctx):
        self.sid       = config['strategy_id']
        self.symbol    = config['symbol']
        self.arena     = config.get('arena', 'HK')
        self.trade_ctx = trade_ctx
        self.quote_ctx = quote_ctx

        params = config.get('conditions', {}).get('params', {})
        self.grid_levels_n    = int(params.get('grid_levels', 4))
        self.grid_spacing_pct = float(params.get('grid_spacing_pct', 0.5))
        self.tp_pct           = float(params.get('tp_pct', 0.5))
        self.sl_boundary_pct  = float(params.get('sl_boundary_pct', 3.0))

        risk = config.get('risk', {})
        self.lots     = int(risk.get('lots', 100))
        # Enforce minimum board lot
        min_lot = {'HK': 100, 'HK_ETF': 500, 'SG': 100}.get(self.arena, 1)
        if self.lots < min_lot:
            logger.warning('[%s] lots=%d < min_lot=%d for %s — forcing to %d',
                           self.sid, self.lots, min_lot, self.arena, min_lot)
            self.lots = min_lot

        self.buy_levels       = []
        self.cmp_reference    = 0.0
        self.completed_cycles = 0
        self.position_qty     = 0
        self.initialized      = False
        self._init_lock       = threading.Lock()
        self.tracked_order_ids = set()
        self._processed_oids  = set()   # guard against duplicate on_fill calls

        logger.info('[%s] GridState init: %s levels=%d spacing=%.1f%% tp=%.1f%% sl_boundary=%.1f%% lots=%d',
                    self.sid, self.symbol, self.grid_levels_n,
                    self.grid_spacing_pct, self.tp_pct, self.sl_boundary_pct, self.lots)

    # ── Called on every price tick ──────────────────────────────────────────

    def on_tick(self, price):
        if not self.initialized:
            with self._init_lock:
                if not self.initialized:
                    self._initialize(price)
            return
        self._check_sl(price)

    def _initialize(self, cmp):
        """Place the initial buy ladder below current market price."""
        self.cmp_reference = cmp
        self.buy_levels = []
        spacing = cmp * self.grid_spacing_pct / 100.0

        logger.info('[%s] Building grid around CMP=%.4f', self.sid, cmp)
        logger.info('[%s]   Spacing: %.2f%% = %.4f | Lots: %d',
                    self.sid, self.grid_spacing_pct, spacing, self.lots)

        for i in range(1, self.grid_levels_n + 1):
            raw_entry = cmp - i * spacing
            if self.arena == 'HK':
                entry = hk_tick_round(raw_entry)
                tp    = hk_tick_round_sell(entry * (1 + self.tp_pct / 100))
            elif self.arena in ('SG', 'SI'):
                entry = round(raw_entry, 3)
                tp    = round(entry * (1 + self.tp_pct / 100), 3)
            else:
                entry = round(raw_entry, 2)
                tp    = round(entry * (1 + self.tp_pct / 100), 2)

            self.buy_levels.append({
                'level_id':        f'BUY_{i}',
                'entry_price':     entry,
                'tp_price':        tp,
                'lots':            self.lots,
                'filled':          False,
                'order_id':        '',
                'tp_order_id':     '',
                'entry_fill_price': 0.0,
            })
            logger.info('[%s]   [BUY_%d] entry=%.4f TP=%.4f', self.sid, i, entry, tp)

        logger.info('[%s] Placing %d buy orders...', self.sid, len(self.buy_levels))
        for lv in self.buy_levels:
            oid = self._submit_limit('BUY', lv['lots'], lv['entry_price'])
            lv['order_id'] = oid
            if oid:
                self.tracked_order_ids.add(oid)
            time.sleep(0.3)  # avoid API rate limit

        active = sum(1 for lv in self.buy_levels if lv['order_id'])
        logger.info('[%s] Grid placed: %d/%d active. Sells placed on fill.',
                    self.sid, active, len(self.buy_levels))
        self.initialized = True

    # ── Called when an order is filled ─────────────────────────────────────

    def on_fill(self, order_id, fill_price, fill_qty, side_str):
        # Guard against duplicate callbacks (dry run threads + quote ticks)
        if order_id in self._processed_oids:
            return
        self._processed_oids.add(order_id)
        # Prevent set growing unbounded
        if len(self._processed_oids) > 2000:
            self._processed_oids = set(list(self._processed_oids)[-500:])

        for lv in self.buy_levels:
            # BUY fill → place TP SELL
            if lv['order_id'] == order_id and not lv['filled']:
                lv['filled'] = True
                lv['entry_fill_price'] = fill_price
                self.position_qty += fill_qty
                self.tracked_order_ids.discard(order_id)

                # Fee deduction (HK stocks only for now)
                buy_fees = calc_hk_fees(fill_price, fill_qty, 'buy') if self.arena == 'HK' else 0.0
                pnl = -buy_fees  # cost of entry
                log_trade(self.sid, self.symbol, 'BOT', fill_qty, fill_price, pnl,
                          signal='grid_entry', bar_date=str(datetime.now(timezone.utc)),
                          indicator_values=f'level={lv["level_id"]},cmp={self.cmp_reference:.4f},fees={buy_fees:.2f}',
                          bar_close=fill_price, order_id=order_id)

                if self.arena == 'HK':
                    tp_price = hk_tick_round_sell(fill_price * (1 + self.tp_pct / 100))
                elif self.arena in ('SG', 'SI'):
                    tp_price = round(fill_price * (1 + self.tp_pct / 100), 3)
                else:
                    tp_price = round(fill_price * (1 + self.tp_pct / 100), 2)

                tp_oid = self._submit_limit('SELL', fill_qty, tp_price)
                lv['tp_order_id'] = tp_oid
                if tp_oid:
                    self.tracked_order_ids.add(tp_oid)

                logger.info('[%s] [%s] Bought @ %.4f → TP SELL @ %.4f (buy fees HKD%.2f)',
                            self.sid, lv['level_id'], fill_price, tp_price, buy_fees)
                return

            # SELL fill → record PnL + replenish buy
            if lv['tp_order_id'] == order_id and lv['filled']:
                entry_p = lv['entry_fill_price']
                sell_fees = calc_hk_fees(fill_price, fill_qty, 'sell') if self.arena == 'HK' else 0.0
                gross_pnl = (fill_price - entry_p) * fill_qty
                net_pnl   = gross_pnl - sell_fees
                self.position_qty -= fill_qty
                self.completed_cycles += 1
                self.tracked_order_ids.discard(order_id)

                log_trade(self.sid, self.symbol, 'SLD', fill_qty, fill_price, round(net_pnl, 4),
                          signal='grid_exit', bar_date=str(datetime.now(timezone.utc)),
                          indicator_values=f'level={lv["level_id"]},cycle={self.completed_cycles},gross={gross_pnl:.2f},fees={sell_fees:.2f}',
                          bar_close=fill_price, order_id=order_id)

                logger.info('[%s] [%s] Sold @ %.4f Gross=$%.2f Fees=HKD%.2f Net=$%.2f cycle#%d',
                            self.sid, lv['level_id'], fill_price,
                            gross_pnl, sell_fees, net_pnl, self.completed_cycles)

                # Replenish buy level
                lv['filled'] = False
                lv['entry_fill_price'] = 0.0
                lv['tp_order_id'] = ''
                new_oid = self._submit_limit('BUY', lv['lots'], lv['entry_price'])
                lv['order_id'] = new_oid
                if new_oid:
                    self.tracked_order_ids.add(new_oid)
                logger.info('[%s] [%s] Buy replenished @ %.4f',
                            self.sid, lv['level_id'], lv['entry_price'])
                return

    # ── SL boundary check ──────────────────────────────────────────────────

    def _check_sl(self, price):
        if not self.buy_levels:
            return
        lowest = self.buy_levels[-1]['entry_price']
        sl_price = lowest * (1 - self.sl_boundary_pct / 100)
        if price < sl_price:
            logger.warning('[%s] SL BOUNDARY HIT: %.4f < %.4f — flattening',
                           self.sid, price, sl_price)
            self._flatten('SL boundary')

    def _flatten(self, reason):
        logger.info('[%s] FLATTEN: %s', self.sid, reason)
        if DRY_RUN:
            logger.info('[%s] [DRY RUN] Would cancel all orders and market sell %d shares',
                        self.sid, self.position_qty)
            self.buy_levels = []
            self.position_qty = 0
            return

        from longport.openapi import OrderSide, OrderType, TimeInForceType
        for lv in self.buy_levels:
            for key in ('order_id', 'tp_order_id'):
                oid = lv.get(key, '')
                if oid:
                    try:
                        self.trade_ctx.cancel_order(oid)
                    except Exception:
                        pass
                    self.tracked_order_ids.discard(oid)
            lv['filled'] = False

        if self.position_qty > 0:
            try:
                from longport.openapi import OrderSide, OrderType, TimeInForceType
                self.trade_ctx.submit_order(
                    symbol=self.symbol,
                    order_type=OrderType.MO,
                    side=OrderSide.Sell,
                    submitted_quantity=self.position_qty,
                    time_in_force=TimeInForceType.Day,
                )
                logger.info('[%s] Market sell %d shares submitted', self.sid, self.position_qty)
            except Exception as e:
                logger.error('[%s] Flatten close error: %s', self.sid, e)

        self.buy_levels = []
        self.position_qty = 0

    def shutdown(self):
        if self.initialized and self.buy_levels:
            self._flatten('Shutdown')

    # ── Order submission helper ─────────────────────────────────────────────

    def _submit_limit(self, action, qty, price):
        """Submit a limit order. Returns order_id string, or '' on failure."""
        if DRY_RUN:
            fake_oid = f'DRY_{self.sid}_{action}_{datetime.now(timezone.utc):%H%M%S%f}'
            logger.info('[DRY RUN][%s] SIMULATED LIMIT %s %d %s @ %.4f → oid=%s',
                        self.sid, action, qty, self.symbol, price, fake_oid)
            # In dry run, simulate immediate fill after a short delay
            def _simulate_fill():
                time.sleep(1.5)
                side_str = 'buy' if action == 'BUY' else 'sell'
                self.on_fill(fake_oid, price, qty, side_str)
            threading.Thread(target=_simulate_fill, daemon=True).start()
            return fake_oid

        from longport.openapi import OrderSide, OrderType, TimeInForceType
        try:
            side = OrderSide.Buy if action == 'BUY' else OrderSide.Sell
            resp = self.trade_ctx.submit_order(
                symbol=self.symbol,
                order_type=OrderType.LO,
                side=side,
                submitted_quantity=qty,
                time_in_force=TimeInForceType.Day,
                submitted_price=decimal.Decimal(str(price)),
                remark=f'QuantX {self.sid}',
            )
            oid = str(resp.order_id)
            logger.info('[%s] LIMIT %s %d %s @ %.4f | oid=%s',
                        self.sid, action, qty, self.symbol, price, oid)
            return oid
        except Exception as e:
            logger.error('[%s] Order error: %s', self.sid, e)
            return ''


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    logger.info('='*72)
    logger.info('QuantX LongPort Master Bot -- %s', EMAIL)
    logger.info('Strategies: %d', len(STRATEGIES))
    if DRY_RUN:
        logger.info('*** DRY RUN MODE -- no real orders will be placed ***')
    logger.info('='*72)

    # LongPort imports
    try:
        from longport.openapi import (
            Config, QuoteContext, TradeContext, SubType, TopicType,
            OrderSide, OrderType, TimeInForceType, OrderStatus,
        )
    except ImportError:
        logger.error('longport package not installed')
        return

    # ONE connection for everything
    cfg = Config(app_key=APP_KEY, app_secret=APP_SECRET, access_token=ACCESS_TOKEN)
    quote_ctx = QuoteContext(cfg)
    if DRY_RUN:
        trade_ctx = None
        logger.info('DRY RUN: QuoteContext connected (TradeContext skipped)')
    else:
        trade_ctx = TradeContext(cfg)
        logger.info('LongPort connected (2 connections: quote + trade)')

    # ── Classify strategies: signal-based vs grid ──────────────────────────
    # The SIGNAL_FUNCTIONS_BLOCK block defines compute_signals_XXXX for each strategy
    signal_states = []   # StrategyState — bar poll loop
    grid_states   = []   # GridState     — event-driven quote subscription

    for s in STRATEGIES:
        library_id = s.get('library_id', '')
        conditions = s.get('conditions', {})
        is_grid = (library_id == 'SYMMETRIC_GRID' or
                   conditions.get('type') == 'SYMMETRIC_GRID')

        if is_grid:
            gs = GridState(s, trade_ctx, quote_ctx)
            grid_states.append(gs)
        else:
            fn_name = f'compute_signals_{s["strategy_id"]}'
            fn = globals().get(fn_name)
            if fn is None:
                logger.warning('[%s] No compute function %s, skipping', s['strategy_id'], fn_name)
                continue
            st = StrategyState(s, fn)
            signal_states.append(st)
            logger.info('[%s] %s lot=%d tp=%.1f%% sl=%.1f%%',
                        st.sid, st.symbol, st.lot_size, st.tp_pct*100, st.sl_pct*100)

    if not signal_states and not grid_states:
        logger.error('No valid strategies. Exiting.')
        return

    all_symbols = list(set(
        [st.symbol for st in signal_states] +
        [gs.symbol for gs in grid_states]
    ))
    logger.info('Symbols: %s | Signal: %d | Grid: %d',
                all_symbols, len(signal_states), len(grid_states))

    # ── Initial quotes ─────────────────────────────────────────────────────
    try:
        quotes = quote_ctx.quote(all_symbols)
        for q in quotes:
            logger.info('Initial: %s = %s', q.symbol, q.last_done)
    except Exception as e:
        logger.warning('Initial quote fetch failed: %s', e)

    # ── Warmup bars for signal strategies ─────────────────────────────────
    from longport.openapi import Period, AdjustType
    period_map = {'1min': Period.Min_1, '5min': Period.Min_5,
                  '15min': Period.Min_15, '30min': Period.Min_30,
                  '1hour': Period.Min_60, '4hour': Period.Min_60,
                  '1day': Period.Day, '1week': Period.Week}

    for st in signal_states:
        try:
            p = period_map.get(st.timeframe, Period.Day)
            candles = quote_ctx.candlesticks(st.symbol, p, 200, AdjustType.ForwardAdjust)
            bars = [{'date': str(c.timestamp), 'open': float(c.open),
                     'high': float(c.high), 'low': float(c.low),
                     'close': float(c.close), 'volume': float(c.volume)}
                    for c in candles]
            st.data_buffer = deque(bars, maxlen=500)
            logger.info('[%s] Warmup: %d bars, last close=%.4f',
                        st.sid, len(bars), bars[-1]['close'])
        except Exception as e:
            logger.warning('[%s] Warmup failed: %s', st.sid, e)

    # ── Position reconciliation (signal strategies, live only) ────────────
    if DRY_RUN:
        logger.info('DRY RUN: skipping position reconciliation')
    elif signal_states:
        logger.info('Reconciling positions with LongPort...')
        try:
            lp_positions = {}
            stock_positions = trade_ctx.stock_positions()
            for pos in stock_positions.channels:
                for p in pos.positions:
                    lp_positions[str(p.symbol)] = int(p.quantity or 0)
            logger.info('LongPort positions: %s', lp_positions)
            for st in signal_states:
                lp_qty = lp_positions.get(st.symbol, 0)
                if st.current_position == 1 and lp_qty == 0:
                    logger.warning('[%s] State LONG but LP FLAT -- resetting', st.sid)
                    st.current_position = 0; st.entry_price = 0.0; st.save_state()
                elif st.current_position == -1 and lp_qty == 0:
                    logger.warning('[%s] State SHORT but LP FLAT -- resetting', st.sid)
                    st.current_position = 0; st.entry_price = 0.0; st.save_state()
                elif st.current_position == 0 and lp_qty > 0:
                    logger.warning('[%s] LP qty=%d but state FLAT -- manual review needed',
                                   st.sid, lp_qty)
                else:
                    logger.info('[%s] Position OK: state=%d lp_qty=%d',
                                st.sid, st.current_position, lp_qty)
        except Exception as e:
            logger.warning('Position reconciliation failed (non-fatal): %s', e)

    # ── Wire grid event callbacks ──────────────────────────────────────────
    if grid_states:
        grid_symbol_map = {}
        for gs in grid_states:
            grid_symbol_map.setdefault(gs.symbol, []).append(gs)

        if not DRY_RUN and trade_ctx:
            def _on_order_changed(event):
                if event.status not in (OrderStatus.Filled, OrderStatus.PartialFilled):
                    return
                oid       = str(event.order_id)
                fill_price = float(event.executed_price or 0)
                fill_qty  = int(event.executed_quantity or 0)
                side_str  = 'buy' if event.side == OrderSide.Buy else 'sell'
                for gs in grid_states:
                    if oid in gs.tracked_order_ids:
                        gs.on_fill(oid, fill_price, fill_qty, side_str)
                        return
            trade_ctx.set_on_order_changed(_on_order_changed)
            trade_ctx.subscribe([TopicType.Private])
            logger.info('Grid: order fill callbacks wired')

        def _on_quote(symbol, event):
            price = float(event.last_done or 0)
            if price > 0:
                for gs in grid_symbol_map.get(symbol, []):
                    gs.on_tick(price)

        quote_ctx.set_on_quote(_on_quote)
        grid_symbols = list(grid_symbol_map.keys())
        try:
            quote_ctx.subscribe(grid_symbols, [SubType.Quote])
            logger.info('Grid: subscribed to real-time quotes: %s', grid_symbols)
        except Exception as e:
            logger.warning('Grid quote subscribe failed: %s', e)

        # Kick off grid init with current price
        try:
            for q in quote_ctx.quote(grid_symbols):
                price = float(q.last_done or 0)
                if price > 0:
                    logger.info('Grid init tick: %s = %.4f', q.symbol, price)
                    for gs in grid_symbol_map.get(q.symbol, []):
                        gs.on_tick(price)
        except Exception as e:
            logger.warning('Grid init quote failed: %s -- will init on first tick', e)

    # ── Order placement helper (signal strategies) ─────────────────────────
    def place_order(st, action, signal='', bar_date='', indicator_values='', bar_close=0.0):
        try:
            quotes = quote_ctx.quote([st.symbol])
            price = float(quotes[0].last_done) if quotes else 0
            if price <= 0:
                logger.warning('[%s] No price, cannot place order', st.sid)
                return
            if st.symbol.endswith('.HK'):
                limit_price = (hk_tick_round(price * 0.998) if action == 'BUY'
                               else hk_tick_round_sell(price * 1.002))
            elif st.symbol.endswith('.SG') or st.symbol.endswith('.SI'):
                limit_price = round(price * (0.998 if action == 'BUY' else 1.002), 3)
            else:
                limit_price = round(price * (0.998 if action == 'BUY' else 1.002), 2)

            if DRY_RUN:
                order_id = f'DRY_{st.sid}_{datetime.now(timezone.utc):%H%M%S%f}'
                logger.info('[DRY RUN][%s] SIMULATED %s %d %s @ %.4f | signal=%s',
                            st.sid, action, st.lot_size, st.symbol, limit_price, signal)
                pnl = 0.0
                if action == 'SELL' and st.current_position == 1 and st.entry_price > 0:
                    pnl = (limit_price - st.entry_price) * st.lot_size
                elif action == 'BUY' and st.current_position == -1 and st.entry_price > 0:
                    pnl = (st.entry_price - limit_price) * st.lot_size
                if action == 'BUY': st.current_position = 1; st.entry_price = limit_price
                else: st.current_position = 0; st.entry_price = 0.0
                st.save_state()
                log_trade(st.sid, st.symbol, 'DRY_BOT' if action == 'BUY' else 'DRY_SLD',
                          st.lot_size, limit_price, pnl, signal, bar_date=bar_date,
                          indicator_values=indicator_values, bar_close=bar_close, order_id=order_id)
                return

            side = OrderSide.Buy if action == 'BUY' else OrderSide.Sell
            resp = trade_ctx.submit_order(
                symbol=st.symbol, order_type=OrderType.LO, side=side,
                submitted_quantity=st.lot_size, time_in_force=TimeInForceType.Day,
                submitted_price=decimal.Decimal(str(limit_price)), remark=f'QuantX {st.sid}')
            order_id = str(resp.order_id)
            logger.info('[%s] ORDER %s %d %s @ %.4f | oid=%s | signal=%s',
                        st.sid, action, st.lot_size, st.symbol, limit_price, order_id, signal)
            pnl = 0.0
            if action == 'SELL' and st.current_position == 1 and st.entry_price > 0:
                pnl = (price - st.entry_price) * st.lot_size
            elif action == 'BUY' and st.current_position == -1 and st.entry_price > 0:
                pnl = (st.entry_price - price) * st.lot_size
            if action == 'BUY': st.current_position = 1; st.entry_price = price
            else: st.current_position = 0; st.entry_price = 0.0
            st.save_state()
            log_trade(st.sid, st.symbol, 'BOT' if action == 'BUY' else 'SLD',
                      st.lot_size, price, pnl, signal, bar_date=bar_date,
                      indicator_values=indicator_values, bar_close=bar_close, order_id=order_id)
        except Exception as e:
            logger.error('[%s] Order failed: %s', st.sid, e)

    def capture_indicator_context(buf_list):
        if not buf_list: return ''
        try:
            close = [b['close'] for b in buf_list]
            parts = [f'bar_close={close[-1]:.4f}']
            try:
                e20 = calc_ema(close, 20)
                if e20[-1] is not None: parts.append(f'ema_20={e20[-1]:.4f}')
            except Exception: pass
            try:
                r14 = calc_rsi(close, 14)
                if r14[-1] is not None: parts.append(f'rsi_14={r14[-1]:.2f}')
            except Exception: pass
            return ','.join(parts)
        except Exception: return ''

    # ── Bar poll loop (signal strategies only) ─────────────────────────────
    def bar_loop():
        if not signal_states:
            logger.info('Bar loop: no signal strategies, exiting loop')
            return
        logger.info('Bar loop started for %d signal strategies', len(signal_states))
        last_dates = {st.sid: (list(st.data_buffer)[-1]['date'] if st.data_buffer else '')
                      for st in signal_states}
        while not _shutdown.is_set():
            _shutdown.wait(60)
            if _shutdown.is_set(): break
            for st in signal_states:
                try:
                    p = period_map.get(st.timeframe, Period.Day)
                    candles = quote_ctx.candlesticks(st.symbol, p, 10, AdjustType.ForwardAdjust)
                    bars = [{'date': str(c.timestamp), 'open': float(c.open),
                             'high': float(c.high), 'low': float(c.low),
                             'close': float(c.close), 'volume': float(c.volume)}
                            for c in candles]
                    new = [b for b in bars if b['date'] > last_dates.get(st.sid, '')]
                    if not new: continue
                    st.data_buffer.extend(new)
                    last_dates[st.sid] = new[-1]['date']
                    logger.info('[%s] Bar: %s close=%.4f',
                                st.sid, new[-1]['date'], new[-1]['close'])
                    buf_list = list(st.data_buffer)
                    sigs = st.compute_fn(buf_list)
                    if not sigs or sigs[-1] is None: continue
                    sig     = sigs[-1]
                    price   = buf_list[-1]['close']
                    bar_dt  = buf_list[-1]['date']
                    ind_ctx = capture_indicator_context(buf_list)
                    if sig == 'buy' and st.current_position == 0:
                        place_order(st, 'BUY', 'entry_long', bar_dt, ind_ctx, price)
                    elif sig == 'sell' and st.current_position == 1:
                        place_order(st, 'SELL', 'exit_long', bar_dt, ind_ctx, price)
                    elif sig == 'short' and st.current_position == 0 and st.has_short:
                        place_order(st, 'SELL', 'entry_short', bar_dt, ind_ctx, price)
                    elif sig == 'cover' and st.current_position == -1 and st.has_short:
                        place_order(st, 'BUY', 'exit_short', bar_dt, ind_ctx, price)
                    else:
                        if st.current_position != 0 and st.entry_price > 0:
                            if st.current_position == 1:
                                if st.sl_pct > 0 and price <= st.entry_price*(1-st.sl_pct):
                                    place_order(st, 'SELL', 'stop_loss', bar_dt, ind_ctx, price)
                                elif st.tp_pct > 0 and price >= st.entry_price*(1+st.tp_pct):
                                    place_order(st, 'SELL', 'take_profit', bar_dt, ind_ctx, price)
                            elif st.current_position == -1:
                                if st.sl_pct > 0 and price >= st.entry_price*(1+st.sl_pct):
                                    place_order(st, 'BUY', 'stop_loss', bar_dt, ind_ctx, price)
                                elif st.tp_pct > 0 and price <= st.entry_price*(1-st.tp_pct):
                                    place_order(st, 'BUY', 'take_profit', bar_dt, ind_ctx, price)
                except Exception as e:
                    logger.exception('[%s] Bar loop error: %s', st.sid, e)

    # ── Heartbeat ──────────────────────────────────────────────────────────
    def heartbeat_loop():
        while not _shutdown.is_set():
            _shutdown.wait(HEARTBEAT_INTERVAL)
            if _shutdown.is_set(): break
            for st in signal_states:
                d = {1:'LONG',-1:'SHORT',0:'FLAT'}.get(st.current_position,'?')
                logger.info('[HB][%s] pos=%s entry=%.4f', st.sid, d, st.entry_price)
            for gs in grid_states:
                logger.info('[HB][%s] initialized=%s position=%d cycles=%d',
                            gs.sid, gs.initialized, gs.position_qty, gs.completed_cycles)

    # ── Start ──────────────────────────────────────────────────────────────
    bar_t = threading.Thread(target=bar_loop, daemon=True)
    hb_t  = threading.Thread(target=heartbeat_loop, daemon=True)
    bar_t.start()
    hb_t.start()

    mode_str = 'DRY RUN' if DRY_RUN else 'LIVE'
    logger.info('Bot is %s. %d signal + %d grid strategies, %d symbols, %s.',
                mode_str, len(signal_states), len(grid_states), len(all_symbols),
                '1 LP connection (quote only)' if DRY_RUN else '2 LP connections')

    try:
        while not _shutdown.is_set():
            _shutdown.wait(1)
    except KeyboardInterrupt:
        pass

    logger.info('Shutting down...')
    for st in signal_states:
        st.save_state()
    for gs in grid_states:
        gs.shutdown()
    logger.info('Bot stopped.')


if __name__ == '__main__':
    main()
'''
