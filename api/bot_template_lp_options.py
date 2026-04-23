"""QuantX -- LongPort Options Bot Template.

Uses __PLACEHOLDER__ substitution for safe code generation.
Supports: SHORT_PUT_SPREAD, SHORT_CALL_SPREAD, IRON_CONDOR,
          SHORT_STRANGLE, LONG_CALL, LONG_PUT, CUSTOM

Key design decisions:
  - BUY long legs FIRST, then SELL short legs (margin management)
  - Reuses QuoteContext + TradeContext (no per-call reconnect)
  - LONGBRIDGE_LOG_PATH env var for LongPort support diagnostics
  - Fetches option chain fresh each day (never hardcodes symbols)
  - Heartbeats to QuantX central for instructor monitoring
"""

LP_OPTIONS_BOT_TEMPLATE = r'''#!/usr/bin/env python3
"""
================================================================================
Strategy     : __STRATEGY_NAME__
Underlying   : __UNDERLYING__
Strategy Type: __STRATEGY_TYPE__
Strike Method: __STRIKE_METHOD__
DTE Target   : __TARGET_DTE__ days
Generated    : by QuantX Deployer
================================================================================
"""
import csv, decimal, json, logging, os, sys, time, traceback, requests
from datetime import datetime, date, timedelta, time as dtime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pytz
from longport.openapi import (
    Config, QuoteContext, TradeContext,
    OrderType, OrderSide, TimeInForceType, OrderStatus,
)

# -- Configuration -----------------------------------------------------------
STRATEGY_NAME   = '__STRATEGY_NAME__'
EMAIL           = '__EMAIL__'
CENTRAL_API_URL = '__CENTRAL_API_URL__'

APP_KEY         = '__APP_KEY__'
APP_SECRET      = '__APP_SECRET__'
ACCESS_TOKEN    = '__ACCESS_TOKEN__'

UNDERLYING      = '__UNDERLYING__'          # e.g. 'SPY.US'
STRATEGY_TYPE   = '__STRATEGY_TYPE__'       # SHORT_PUT_SPREAD | IRON_CONDOR | etc.
STRIKE_METHOD   = '__STRIKE_METHOD__'       # DELTA | POINTS_OTM | PCT_OTM | WING_POINTS
TARGET_DTE      = __TARGET_DTE__            # e.g. 7
DTE_TOLERANCE   = __DTE_TOLERANCE__         # e.g. 2

# Strike values
PUT_DELTA       = __PUT_DELTA__             # e.g. -0.30
CALL_DELTA      = __CALL_DELTA__            # e.g.  0.30
PUT_WING_WIDTH  = __PUT_WING_WIDTH__        # points wide e.g. 5.0
CALL_WING_WIDTH = __CALL_WING_WIDTH__       # points wide e.g. 5.0
CUSTOM_LEGS     = __CUSTOM_LEGS__           # list of leg dicts for CUSTOM strategy

# Entry/Exit
ENTRY_TIME_ET   = '__ENTRY_TIME_ET__'       # 'HH:MM' e.g. '09:45'
EXIT_TIME_ET    = '__EXIT_TIME_ET__'        # 'HH:MM' e.g. '15:45'
ENTRY_DAYS      = __ENTRY_DAYS__            # e.g. ['Mon','Tue','Wed','Thu','Fri']

# Risk management
PROFIT_TARGET_PCT  = __PROFIT_TARGET_PCT__  # e.g. 0.50 = close at 50% max profit
STOP_LOSS_MULT     = __STOP_LOSS_MULT__     # e.g. 2.0 = close when loss = 2x credit
CONTRACTS          = __CONTRACTS__          # number of spreads
MAX_DAILY_LOSS     = __MAX_DAILY_LOSS__     # dollar amount to halt trading
DRY_RUN            = __DRY_RUN__            # True = log only, no real orders

# Timing
MONITOR_INTERVAL   = 60    # seconds between P&L checks
HEARTBEAT_SECS     = 300   # seconds between heartbeats to central
ORDER_TIMEOUT      = 60    # seconds to wait for fill

# Paths
LOG_DIR    = '__LOG_DIR__'
TRADES_DIR = '__TRADES_DIR__'
STATE_DIR  = '__STATE_DIR__'
os.makedirs(LOG_DIR,    exist_ok=True)
os.makedirs(TRADES_DIR, exist_ok=True)
os.makedirs(STATE_DIR,  exist_ok=True)

TRADES_FILE   = os.path.join(TRADES_DIR, f'trades_{STRATEGY_NAME}_all.csv')
POSITION_FILE = os.path.join(STATE_DIR,  f'pos_{STRATEGY_NAME}.json')
RISK_FILE     = os.path.join(STATE_DIR,  f'risk_{STRATEGY_NAME}.json')

ET = pytz.timezone('US/Eastern')

# -- Logging -----------------------------------------------------------------
logger = logging.getLogger(STRATEGY_NAME)
logger.setLevel(logging.INFO)
_fh = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, f'{STRATEGY_NAME}.log'),
    when='midnight', backupCount=30, encoding='utf-8'
)
_fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
_ch = logging.StreamHandler()
_ch.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(_fh)
logger.addHandler(_ch)

# -- LongPort connection -----------------------------------------------------
# Reuse contexts -- do NOT create new ones per trade (connection limit)
_cfg = None
_quote_ctx = None
_trade_ctx = None

def get_config():
    global _cfg
    if _cfg is None:
        kwargs = dict(app_key=APP_KEY, app_secret=APP_SECRET, access_token=ACCESS_TOKEN)
        log_path = os.environ.get('LONGBRIDGE_LOG_PATH', '')
        if log_path:
            os.makedirs(log_path, exist_ok=True)
            kwargs['log_path'] = log_path
        _cfg = Config(**kwargs)
    return _cfg

def get_quote_ctx():
    global _quote_ctx
    if _quote_ctx is None:
        _quote_ctx = QuoteContext(get_config())
        logger.info('[LP] QuoteContext created')
    return _quote_ctx

def get_trade_ctx():
    global _trade_ctx
    if _trade_ctx is None:
        _trade_ctx = TradeContext(get_config())
        logger.info('[LP] TradeContext created')
    return _trade_ctx

def reset_connections():
    """Call when connection errors occur -- forces reconnect on next use."""
    global _quote_ctx, _trade_ctx
    _quote_ctx = None
    _trade_ctx = None
    logger.warning('[LP] Connections reset -- will reconnect on next use')

# -- Time helpers ------------------------------------------------------------
WEEKDAY_MAP = {'Mon':0,'Tue':1,'Wed':2,'Thu':3,'Fri':4,'Sat':5,'Sun':6}

def now_et():
    return datetime.now(ET)

def parse_time(s):
    h, m = s.split(':')
    return dtime(int(h), int(m))

def is_entry_day():
    wd = now_et().weekday()
    return wd in {WEEKDAY_MAP[d] for d in ENTRY_DAYS if d in WEEKDAY_MAP}

def is_entry_time():
    t = now_et().time()
    return parse_time(ENTRY_TIME_ET) <= t <= parse_time(EXIT_TIME_ET)

def past_exit_time():
    return now_et().time() >= parse_time(EXIT_TIME_ET)

# -- Option chain helpers ----------------------------------------------------

def get_target_expiry():
    """Find expiry closest to TARGET_DTE from today."""
    ctx = get_quote_ctx()
    expiries = ctx.option_chain_expiry_date_list(UNDERLYING)
    today = date.today()
    future = [e for e in expiries
              if isinstance(e, date) and e > today]
    if not future:
        raise RuntimeError('No future expiries found')

    for exp in sorted(future):
        dte = (exp - today).days
        if abs(dte - TARGET_DTE) <= DTE_TOLERANCE:
            logger.info(f'[CHAIN] Target expiry: {exp} ({dte} DTE)')
            return exp

    best = min(future, key=lambda e: abs((e - today).days - TARGET_DTE))
    logger.warning(f'[CHAIN] No expiry within tolerance, using {best}')
    return best


def get_chain(expiry):
    """Get option chain for expiry. Returns list of StrikePriceInfo."""
    ctx = get_quote_ctx()
    chain = ctx.option_chain_info_by_date(UNDERLYING, expiry)
    logger.info(f'[CHAIN] {len(chain)} strikes for {expiry}')
    return chain


def get_spot():
    """Get current underlying price."""
    ctx = get_quote_ctx()
    quotes = ctx.quote([UNDERLYING])
    if not quotes:
        raise RuntimeError(f'No quote for {UNDERLYING}')
    return float(str(quotes[0].last_done))


def pick_strike(chain, right, method, value, spot=None, anchor_strike=None):
    """
    Select a strike from the chain.

    right:         'PUT' or 'CALL'
    method:        DELTA | PCT_OTM | POINTS_OTM | WING_POINTS
    value:         float (delta=-0.30, pct=0.05, points=5)
    spot:          current underlying price (needed for PCT_OTM, POINTS_OTM)
    anchor_strike: float, required for WING_POINTS

    Returns: (symbol, strike_price)
    """
    if spot is None:
        spot = get_spot()

    if method == 'DELTA':
        # delta ~ -0.30 put -> approximately 3-4% OTM
        abs_delta = abs(value)
        if right == 'PUT':
            target = spot * (1 - abs_delta * 0.8)
        else:
            target = spot * (1 + abs_delta * 0.8)

    elif method == 'PCT_OTM':
        if right == 'PUT':
            target = spot * (1 - abs(value))
        else:
            target = spot * (1 + abs(value))

    elif method == 'POINTS_OTM':
        if right == 'PUT':
            target = spot - abs(value)
        else:
            target = spot + abs(value)

    elif method == 'WING_POINTS':
        if anchor_strike is None:
            raise ValueError('WING_POINTS requires anchor_strike')
        if right == 'PUT':
            target = anchor_strike - abs(value)
        else:
            target = anchor_strike + abs(value)

    else:
        raise ValueError(f'Unknown strike method: {method}')

    best_item = min(chain, key=lambda x: abs(float(str(x.price)) - target))
    strike = float(str(best_item.price))
    symbol = best_item.put_symbol if right == 'PUT' else best_item.call_symbol

    if not symbol:
        raise RuntimeError(f'No {right} symbol at strike {strike}')

    logger.info(f'[STRIKE] {right} {method}={value} -> {symbol} (strike ${strike:.1f})')
    return symbol, strike


def build_spread_legs(chain, expiry, spot):
    """
    Build the list of legs for the configured strategy.
    Returns list of dicts: {symbol, right, action, strike, qty}
    Order: long legs first, then short legs (margin management)
    """
    legs_to_buy  = []
    legs_to_sell = []

    if STRATEGY_TYPE == 'SHORT_PUT_SPREAD':
        short_sym, short_k = pick_strike(chain, 'PUT', STRIKE_METHOD, PUT_DELTA, spot)
        long_sym,  long_k  = pick_strike(chain, 'PUT', 'WING_POINTS', PUT_WING_WIDTH,
                                          spot, anchor_strike=short_k)
        legs_to_buy  = [{'symbol':long_sym,  'right':'PUT','action':'BUY', 'strike':long_k,  'qty':CONTRACTS}]
        legs_to_sell = [{'symbol':short_sym, 'right':'PUT','action':'SELL','strike':short_k, 'qty':CONTRACTS}]

    elif STRATEGY_TYPE == 'SHORT_CALL_SPREAD':
        short_sym, short_k = pick_strike(chain, 'CALL', STRIKE_METHOD, CALL_DELTA, spot)
        long_sym,  long_k  = pick_strike(chain, 'CALL', 'WING_POINTS', CALL_WING_WIDTH,
                                          spot, anchor_strike=short_k)
        legs_to_buy  = [{'symbol':long_sym,  'right':'CALL','action':'BUY', 'strike':long_k,  'qty':CONTRACTS}]
        legs_to_sell = [{'symbol':short_sym, 'right':'CALL','action':'SELL','strike':short_k, 'qty':CONTRACTS}]

    elif STRATEGY_TYPE == 'IRON_CONDOR':
        p_short, p_short_k = pick_strike(chain, 'PUT',  STRIKE_METHOD, PUT_DELTA,  spot)
        p_long,  p_long_k  = pick_strike(chain, 'PUT',  'WING_POINTS', PUT_WING_WIDTH,  spot, p_short_k)
        c_short, c_short_k = pick_strike(chain, 'CALL', STRIKE_METHOD, CALL_DELTA, spot)
        c_long,  c_long_k  = pick_strike(chain, 'CALL', 'WING_POINTS', CALL_WING_WIDTH, spot, c_short_k)
        legs_to_buy  = [
            {'symbol':p_long,  'right':'PUT', 'action':'BUY', 'strike':p_long_k,  'qty':CONTRACTS},
            {'symbol':c_long,  'right':'CALL','action':'BUY', 'strike':c_long_k,  'qty':CONTRACTS},
        ]
        legs_to_sell = [
            {'symbol':p_short, 'right':'PUT', 'action':'SELL','strike':p_short_k, 'qty':CONTRACTS},
            {'symbol':c_short, 'right':'CALL','action':'SELL','strike':c_short_k, 'qty':CONTRACTS},
        ]

    elif STRATEGY_TYPE == 'SHORT_STRANGLE':
        p_sym, p_k = pick_strike(chain, 'PUT',  STRIKE_METHOD, PUT_DELTA,  spot)
        c_sym, c_k = pick_strike(chain, 'CALL', STRIKE_METHOD, CALL_DELTA, spot)
        legs_to_sell = [
            {'symbol':p_sym, 'right':'PUT', 'action':'SELL','strike':p_k, 'qty':CONTRACTS},
            {'symbol':c_sym, 'right':'CALL','action':'SELL','strike':c_k, 'qty':CONTRACTS},
        ]

    elif STRATEGY_TYPE == 'LONG_CALL':
        sym, k = pick_strike(chain, 'CALL', STRIKE_METHOD, CALL_DELTA, spot)
        legs_to_buy = [{'symbol':sym,'right':'CALL','action':'BUY','strike':k,'qty':CONTRACTS}]

    elif STRATEGY_TYPE == 'LONG_PUT':
        sym, k = pick_strike(chain, 'PUT', STRIKE_METHOD, PUT_DELTA, spot)
        legs_to_buy = [{'symbol':sym,'right':'PUT','action':'BUY','strike':k,'qty':CONTRACTS}]

    elif STRATEGY_TYPE == 'CUSTOM':
        last_short = {'PUT': None, 'CALL': None}
        for leg in CUSTOM_LEGS:
            right  = leg['right'].upper()
            action = leg['action'].upper()
            method = leg['method'].upper()
            value  = float(leg['value'])
            qty    = int(leg.get('qty', 1)) * CONTRACTS
            anchor = last_short.get(right)

            sym, k = pick_strike(chain, right, method, value, spot, anchor)
            entry = {'symbol':sym,'right':right,'action':action,'strike':k,'qty':qty}

            if action == 'BUY':
                legs_to_buy.append(entry)
            else:
                legs_to_sell.append(entry)
                last_short[right] = k

    else:
        raise ValueError(f'Unknown STRATEGY_TYPE: {STRATEGY_TYPE}')

    # BUY legs first -- critical for margin management
    return legs_to_buy + legs_to_sell


# -- Order placement ---------------------------------------------------------

def get_mid_price(symbol):
    """Get mid price for an option. Returns None if quotes unavailable."""
    try:
        ctx = get_quote_ctx()
        quotes = ctx.option_quote([symbol])
        if not quotes:
            return None
        q = quotes[0]
        bid = float(str(q.bid))
        ask = float(str(q.ask))
        if bid <= 0 and ask <= 0:
            return None
        return max(round((bid + ask) / 2, 2), 0.01)
    except Exception as e:
        logger.warning(f'[QUOTE] {symbol} quote failed: {e}')
        return None


def place_order(symbol, action, qty, limit_price):
    """Place a single-leg limit order. Returns order_id or None."""
    if DRY_RUN:
        logger.info(f'[DRY_RUN] Would {action} {qty}x {symbol} @ ${limit_price:.2f}')
        return f'DRYRUN_{symbol}'

    try:
        ctx = get_trade_ctx()
        side = OrderSide.Buy if action == 'BUY' else OrderSide.Sell
        resp = ctx.submit_order(
            symbol=symbol,
            order_type=OrderType.LO,
            side=side,
            submitted_quantity=decimal.Decimal(str(qty)),
            submitted_price=decimal.Decimal(str(limit_price)),
            time_in_force=TimeInForceType.Day,
            remark=f'QuantX {STRATEGY_NAME}'
        )
        order_id = str(resp.order_id)
        logger.info(f'[ORDER] {action} {qty}x {symbol} @ ${limit_price:.2f} -> {order_id}')
        return order_id
    except Exception as e:
        logger.error(f'[ORDER] Failed {action} {symbol}: {e}')
        return None


def wait_for_fill(order_id, timeout=ORDER_TIMEOUT):
    """Wait until order fills or timeout. Returns True if filled."""
    if DRY_RUN or order_id.startswith('DRYRUN'):
        return True

    start = time.time()
    while time.time() - start < timeout:
        try:
            ctx = get_trade_ctx()
            orders = ctx.today_orders()
            for o in orders:
                if str(o.order_id) == order_id:
                    status = str(o.status).split('.')[-1]
                    if status == 'FilledStatus':
                        fill = float(str(o.executed_price))
                        logger.info(f'[FILL] {order_id} filled @ ${fill:.2f}')
                        return True
                    elif status in ('Failed', 'Rejected', 'Cancelled'):
                        logger.error(f'[FILL] {order_id} status: {status}')
                        return False
        except Exception as e:
            logger.warning(f'[FILL] Check error: {e}')
        time.sleep(3)

    logger.error(f'[FILL] {order_id} timeout after {timeout}s')
    return False


def enter_position(legs, spot):
    """
    Place all legs. BUY legs must come first (already ordered by build_spread_legs).
    Returns net_credit (positive = credit received).
    """
    logger.info(f'[ENTRY] Entering {STRATEGY_TYPE} | {len(legs)} legs | spot=${spot:.2f}')
    order_ids = []
    prices    = []

    for leg in legs:
        symbol = leg['symbol']
        action = leg['action']
        qty    = leg['qty']

        mid = get_mid_price(symbol)
        if mid is None:
            abs_delta = abs(PUT_DELTA if leg['right'] == 'PUT' else CALL_DELTA)
            mid = max(round(spot * abs_delta * 0.01, 2), 0.05)
            logger.warning(f'[ENTRY] No quote for {symbol}, using estimate ${mid:.2f}')

        oid = place_order(symbol, action, qty, mid)
        if oid is None:
            logger.error(f'[ENTRY] Failed to place {action} {symbol} -- aborting')
            cancel_orders(order_ids)
            return None

        order_ids.append(oid)

        signed_price = mid if action == 'SELL' else -mid
        prices.append(signed_price * qty)

        filled = wait_for_fill(oid)
        if not filled:
            logger.error(f'[ENTRY] {oid} did not fill -- aborting remaining legs')
            cancel_orders(order_ids)
            return None

        time.sleep(1)

    net_credit = sum(prices)
    logger.info(f'[ENTRY] All legs filled | net_credit=${net_credit:.2f}')
    return net_credit


def close_position(legs, reason='EXIT'):
    """Close all legs by reversing actions."""
    logger.info(f'[EXIT] Closing {len(legs)} legs | reason={reason}')
    net_debit = 0.0

    for leg in reversed(legs):
        symbol = leg['symbol']
        close_action = 'BUY' if leg['action'] == 'SELL' else 'SELL'
        qty = leg['qty']

        mid = get_mid_price(symbol)
        if mid is None:
            mid = 0.01
            logger.warning(f'[EXIT] No quote for {symbol}, using ${mid:.2f}')

        oid = place_order(symbol, close_action, qty, mid)
        if oid:
            wait_for_fill(oid)
            signed = mid if close_action == 'BUY' else -mid
            net_debit += signed * qty
        time.sleep(1)

    return net_debit


def cancel_orders(order_ids):
    """Cancel a list of orders (cleanup on partial failure)."""
    if DRY_RUN:
        return
    ctx = get_trade_ctx()
    for oid in order_ids:
        if oid and not oid.startswith('DRYRUN'):
            try:
                ctx.cancel_order(oid)
                logger.info(f'[CANCEL] {oid}')
            except Exception as e:
                logger.warning(f'[CANCEL] {oid} failed: {e}')


# -- State persistence -------------------------------------------------------

def load_state():
    defaults = {
        'position_open': False,
        'legs': [],
        'net_credit': 0.0,
        'entry_time': '',
        'expiry': '',
        'daily_pnl': 0.0,
        'daily_trades': 0,
        'last_reset_date': '',
    }
    try:
        if os.path.exists(POSITION_FILE):
            with open(POSITION_FILE) as f:
                data = json.load(f)
                defaults.update(data)
    except Exception as e:
        logger.warning(f'[STATE] Load error: {e}')
    return defaults


def save_state(state):
    try:
        with open(POSITION_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.warning(f'[STATE] Save error: {e}')


def reset_daily_counters(state):
    today_str = date.today().isoformat()
    if state.get('last_reset_date') != today_str:
        state['daily_pnl']    = 0.0
        state['daily_trades'] = 0
        state['last_reset_date'] = today_str
        logger.info('[STATE] Daily counters reset')
    return state


# -- P&L monitoring ----------------------------------------------------------

def get_position_value(legs):
    """Current market value of all legs (cost to close)."""
    total = 0.0
    for leg in legs:
        mid = get_mid_price(leg['symbol'])
        if mid is None:
            continue
        signed = mid if leg['action'] == 'SELL' else -mid
        total += signed * leg['qty']
    return total


def check_exit_conditions(state):
    """Returns exit reason string if we should close, else None."""
    if not state['position_open']:
        return None

    net_credit = state['net_credit']
    if net_credit == 0:
        return None

    current_value = get_position_value(state['legs'])

    remaining_pct = current_value / net_credit if net_credit > 0 else 1.0
    if remaining_pct <= (1.0 - PROFIT_TARGET_PCT):
        return f'PROFIT_TARGET ({remaining_pct:.1%} remaining)'

    if current_value < 0:
        loss_mult = abs(current_value) / net_credit
        if loss_mult >= STOP_LOSS_MULT:
            return f'STOP_LOSS ({loss_mult:.1f}x loss)'

    if past_exit_time():
        return 'EOD'

    return None


# -- Heartbeat ---------------------------------------------------------------

last_heartbeat = 0

def send_heartbeat(state):
    global last_heartbeat
    if time.time() - last_heartbeat < HEARTBEAT_SECS:
        return
    try:
        requests.post(f'{CENTRAL_API_URL}/api/heartbeat', json={
            'email': EMAIL,
            'strategy_id': STRATEGY_NAME,
            'symbol': UNDERLYING,
            'status': 'running',
            'position_open': state.get('position_open', False),
            'daily_pnl': state.get('daily_pnl', 0),
        }, timeout=5)
        last_heartbeat = time.time()
    except Exception:
        pass


def log_trade_csv(action, symbol, qty, price, pnl=0.0, note=''):
    fields = ['datetime','strategy','symbol','action','qty','price','pnl','note']
    write_header = not os.path.exists(TRADES_FILE)
    with open(TRADES_FILE, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            w.writeheader()
        w.writerow({
            'datetime': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy': STRATEGY_NAME, 'symbol': symbol, 'action': action,
            'qty': qty, 'price': round(price, 4), 'pnl': round(pnl, 2), 'note': note,
        })
    try:
        requests.post(f'{CENTRAL_API_URL}/api/trade', json={
            'email': EMAIL, 'strategy_id': STRATEGY_NAME, 'symbol': UNDERLYING,
            'side': action.lower(), 'price': float(price), 'qty': float(qty),
            'pnl': float(pnl),
        }, timeout=5)
    except Exception:
        pass


# -- Main loop ---------------------------------------------------------------

def main():
    logger.info('=' * 60)
    logger.info(f'QuantX Options Bot -- {STRATEGY_NAME}')
    logger.info(f'Strategy: {STRATEGY_TYPE} | DTE: {TARGET_DTE} | Contracts: {CONTRACTS}')
    logger.info(f'Entry: {ENTRY_TIME_ET} ET | Exit: {EXIT_TIME_ET} ET')
    logger.info(f'DRY_RUN: {DRY_RUN}')
    logger.info('=' * 60)

    try:
        spot = get_spot()
        logger.info(f'[INIT] {UNDERLYING} = ${spot:.2f} | Connections OK')
    except Exception as e:
        logger.error(f'[INIT] Connection failed: {e}')
        sys.exit(1)

    state = load_state()

    while True:
        try:
            state = reset_daily_counters(state)
            send_heartbeat(state)

            now = now_et()

            if state['position_open']:
                reason = check_exit_conditions(state)
                if reason:
                    logger.info(f'[SIGNAL] Exit: {reason}')
                    net_debit = close_position(state['legs'], reason)
                    if net_debit is not None:
                        pnl = state['net_credit'] + net_debit
                        state['daily_pnl'] += pnl
                        state['position_open'] = False
                        state['legs'] = []
                        log_trade_csv('CLOSE', UNDERLYING, CONTRACTS,
                                      abs(net_debit), pnl, reason)
                        logger.info(f'[EXIT] PnL: ${pnl:+.2f} | Daily: ${state["daily_pnl"]:+.2f}')
                        save_state(state)

            elif (is_entry_day() and
                  is_entry_time() and
                  state['daily_trades'] == 0 and
                  state['daily_pnl'] > -MAX_DAILY_LOSS):

                try:
                    spot = get_spot()
                    expiry = get_target_expiry()
                    chain  = get_chain(expiry)
                    legs   = build_spread_legs(chain, expiry, spot)

                    logger.info(f'[SIGNAL] Entry | spot=${spot:.2f} | expiry={expiry}')
                    for leg in legs:
                        logger.info(f'  {leg["action"]} {leg["qty"]}x {leg["symbol"]} (strike ${leg["strike"]:.1f})')

                    net_credit = enter_position(legs, spot)

                    if net_credit is not None:
                        state['position_open'] = True
                        state['legs']          = legs
                        state['net_credit']    = net_credit
                        state['entry_time']    = now.isoformat()
                        state['expiry']        = str(expiry)
                        state['daily_trades'] += 1
                        log_trade_csv('OPEN', UNDERLYING, CONTRACTS,
                                      net_credit, 0.0, f'{STRATEGY_TYPE} {expiry}')
                        logger.info(f'[ENTRY] Net credit: ${net_credit:.2f}')
                        save_state(state)

                except Exception as e:
                    logger.error(f'[ENTRY] Error: {e}')
                    logger.debug(traceback.format_exc())
                    reset_connections()

            time.sleep(MONITOR_INTERVAL)

        except KeyboardInterrupt:
            logger.info('[BOT] Stopped by user')
            break
        except Exception as e:
            logger.error(f'[LOOP] Unexpected error: {e}')
            logger.debug(traceback.format_exc())
            reset_connections()
            time.sleep(30)


if __name__ == '__main__':
    main()
'''
