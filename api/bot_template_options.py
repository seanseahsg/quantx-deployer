"""QuantX — Production Options bot template.
Uses __PLACEHOLDER__ substitution for safe code generation.
Supports vertical spreads, iron condors, strangles, and straddles via IBKR BAG orders.
"""

OPTIONS_BOT_TEMPLATE = r'''#!/usr/bin/env python3
"""
================================================================================
Strategy     : __STRATEGY_NAME__
Underlying   : __UNDERLYING__ (__UNDERLYING_SEC_TYPE__) on __UNDERLYING_EXCHANGE__
Strategy Type: __STRATEGY_TYPE__
Strike Method: __STRIKE_METHOD__
Trading Class: __OPTION_TRADING_CLASS__
Client ID    : __CLIENT_ID__
Mode         : __DRY_RUN__ (DRY_RUN) | Port __PORT__
Generated    : by QuantX Deployer
================================================================================
"""
import asyncio, csv, json, os, math, requests, sys, traceback
from collections import deque
from datetime import datetime, date, timedelta, time as dtime
import pytz
from ib_insync import (IB, Index, Stock, ContFuture, Option, Contract, ComboLeg,
                        MarketOrder, LimitOrder, TagValue, util)
from logging.handlers import TimedRotatingFileHandler
import logging

# ── Configuration ────────────────────────────────────────────────────────────
STRATEGY_NAME       = '__STRATEGY_NAME__'
ACCOUNT_ID          = '__ACCOUNT_ID__'
PORT                = __PORT__
CLIENT_ID           = __CLIENT_ID__
EMAIL               = '__EMAIL__'
CENTRAL_API_URL     = '__CENTRAL_API_URL__'

UNDERLYING          = '__UNDERLYING__'
UNDERLYING_SEC_TYPE = '__UNDERLYING_SEC_TYPE__'
UNDERLYING_EXCHANGE = '__UNDERLYING_EXCHANGE__'
OPTION_TRADING_CLASS = '__OPTION_TRADING_CLASS__'

STRATEGY_TYPE       = '__STRATEGY_TYPE__'       # BULL_PUT_SPREAD, BEAR_CALL_SPREAD, IRON_CONDOR, SHORT_STRANGLE, SHORT_STRADDLE
STRIKE_METHOD       = '__STRIKE_METHOD__'       # DELTA, PCT_OTM, POINTS_OTM
PUT_DELTA           = __PUT_DELTA__             # e.g. -0.16
CALL_DELTA          = __CALL_DELTA__            # e.g.  0.16
PUT_PCT_OTM         = __PUT_PCT_OTM__           # e.g.  0.05 (5% OTM)
CALL_PCT_OTM        = __CALL_PCT_OTM__
PUT_POINTS_OTM      = __PUT_POINTS_OTM__        # e.g.  50 (points below spot)
CALL_POINTS_OTM     = __CALL_POINTS_OTM__
PUT_WING_WIDTH      = __PUT_WING_WIDTH__         # points between short/long put strikes
CALL_WING_WIDTH     = __CALL_WING_WIDTH__        # points between short/long call strikes
MULTIPLIER          = __MULTIPLIER__

ENTRY_TIME_ET       = '__ENTRY_TIME_ET__'        # 'HH:MM'
EXIT_TIME_ET        = '__EXIT_TIME_ET__'         # 'HH:MM'
RESTART_TIME_ET     = '__RESTART_TIME_ET__'      # 'HH:MM' — daily risk-counter reset

SMA_FILTER          = __SMA_FILTER__             # True/False
SMA_PERIOD          = __SMA_PERIOD__
SMA_DIRECTION       = '__SMA_DIRECTION__'        # 'above' or 'below'
IV_FILTER           = __IV_FILTER__              # True/False — VIX range filter
MIN_VIX             = __MIN_VIX__
MAX_VIX             = __MAX_VIX__
SKIP_FOMC           = __SKIP_FOMC__

RISK_PCT            = __RISK_PCT__               # fraction of equity risked per trade
MIN_CONTRACTS       = __MIN_CONTRACTS__
MAX_CONTRACTS       = __MAX_CONTRACTS__
ACCOUNT_EQUITY      = __ACCOUNT_EQUITY__

PROFIT_TARGET_PCT   = __PROFIT_TARGET_PCT__      # close when spread worth <= (1 - pct) * credit, e.g. 0.50 = 50%
LOSS_LIMIT_MULT     = __LOSS_LIMIT_MULT__        # close when debit >= credit * mult, e.g. 2.0
EOD_EXIT            = __EOD_EXIT__               # True/False — force-close at EXIT_TIME_ET

MAX_DAILY_LOSS      = __MAX_DAILY_LOSS__         # dollar amount — halt trading for the day
MAX_ORDERS_PER_DAY  = __MAX_ORDERS_PER_DAY__
DRY_RUN             = __DRY_RUN__                # True = log only, no real orders
ORDER_TIMEOUT       = 60
MONITOR_INTERVAL    = 60                         # seconds between P&L checks
HEARTBEAT_SECS      = 300

ET = pytz.timezone('US/Eastern')

LOG_DIR    = '__LOG_DIR__'
TRADES_DIR = '__TRADES_DIR__'
STATE_DIR  = '__STATE_DIR__'
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TRADES_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

TRADES_FILE   = os.path.join(TRADES_DIR, f'trades_{STRATEGY_NAME}_all.csv')
RISK_FILE     = os.path.join(STATE_DIR, f'risk_{STRATEGY_NAME}.json')
POSITION_FILE = os.path.join(STATE_DIR, f'pos_{STRATEGY_NAME}.json')

CSV_FIELDS = ['execId', 'datetime', 'strategy', 'symbol', 'secType', 'exchange',
              'currency', 'side', 'quantity', 'price', 'commission', 'pnl',
              'orderRef', 'bar_size', 'signal']

# ── FOMC dates (update annually) ────────────────────────────────────────────
FOMC_DATES = set()
try:
    FOMC_DATES = {date.fromisoformat(d) for d in [
        '2026-01-28', '2026-03-18', '2026-04-29', '2026-06-10',
        '2026-07-29', '2026-09-16', '2026-11-04', '2026-12-16',
    ]}
except Exception:
    pass

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(STRATEGY_NAME)
logger.setLevel(logging.INFO)
_fh = TimedRotatingFileHandler(os.path.join(LOG_DIR, f'{STRATEGY_NAME}.log'),
                                when='midnight', backupCount=30, encoding='utf-8')
_fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
_ch = logging.StreamHandler()
_ch.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(_fh)
logger.addHandler(_ch)

# ── Global state ─────────────────────────────────────────────────────────────
ib = IB()
underlying_contract = None
price_buffer = deque(maxlen=500)
risk_state = {}

# Position state — persisted to disk
position_open  = False
position_legs  = []          # list of dicts: {strike, right, action, conId, localSymbol}
net_credit     = 0.0         # per-unit credit received at entry
entry_time_ts  = ''
num_contracts  = 0
entry_expiry   = ''

# ── Time helpers ─────────────────────────────────────────────────────────────

def now_et():
    return datetime.now(ET)

def parse_time(s):
    h, m = s.split(':')
    return dtime(int(h), int(m))

def in_trade_window():
    t = now_et().time()
    return parse_time(ENTRY_TIME_ET) <= t <= parse_time(EXIT_TIME_ET)

def past_exit_time():
    return now_et().time() > parse_time(EXIT_TIME_ET)

def is_fomc_day():
    if not SKIP_FOMC:
        return False
    return now_et().date() in FOMC_DATES

# ── CSV trade logging ────────────────────────────────────────────────────────

def ensure_csv():
    if not os.path.exists(TRADES_FILE):
        with open(TRADES_FILE, 'w', newline='') as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

def log_trade(side, qty, price, commission, exec_id, pnl=0.0, signal=''):
    ensure_csv()
    with open(TRADES_FILE, 'a', newline='') as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow({
            'execId': exec_id,
            'datetime': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy': STRATEGY_NAME, 'symbol': UNDERLYING, 'secType': 'OPT',
            'exchange': UNDERLYING_EXCHANGE, 'currency': 'USD', 'side': side,
            'quantity': qty, 'price': round(price, 4),
            'commission': round(commission, 4), 'pnl': round(pnl, 2),
            'orderRef': STRATEGY_NAME, 'bar_size': STRATEGY_TYPE, 'signal': signal,
        })
    logger.info(f'[TRADE] {side} {qty}x {UNDERLYING} @ {price:.4f} | pnl=${pnl:+.2f} | {signal}')
    try:
        requests.post(f'{CENTRAL_API_URL}/api/trade', json={
            'email': EMAIL, 'strategy_id': STRATEGY_NAME, 'symbol': UNDERLYING,
            'side': 'buy' if side == 'BOT' else 'sell', 'price': float(price),
            'qty': float(qty), 'pnl': float(pnl),
        }, timeout=5)
    except Exception:
        pass

# ── State persistence ────────────────────────────────────────────────────────

def save_state():
    try:
        pos_data = {
            'position_open': position_open,
            'position_legs': position_legs,
            'net_credit': net_credit,
            'entry_time_ts': entry_time_ts,
            'num_contracts': num_contracts,
            'entry_expiry': entry_expiry,
        }
        with open(POSITION_FILE, 'w') as f:
            json.dump(pos_data, f, indent=2)
        with open(RISK_FILE, 'w') as f:
            json.dump(risk_state, f, indent=2)
    except Exception as e:
        logger.warning(f'State save: {e}')

def load_state():
    global position_open, position_legs, net_credit, entry_time_ts, num_contracts, entry_expiry, risk_state
    risk_state = {'daily_loss': 0.0, 'session_pnl': 0.0, 'order_count': 0, 'halted': False}
    try:
        if os.path.exists(POSITION_FILE):
            with open(POSITION_FILE) as f:
                s = json.load(f)
            position_open = bool(s.get('position_open', False))
            position_legs = s.get('position_legs', [])
            net_credit = float(s.get('net_credit', 0.0))
            entry_time_ts = str(s.get('entry_time_ts', ''))
            num_contracts = int(s.get('num_contracts', 0))
            entry_expiry = str(s.get('entry_expiry', ''))
        if os.path.exists(RISK_FILE):
            with open(RISK_FILE) as f:
                saved = json.load(f)
            today = now_et().strftime('%Y-%m-%d')
            if saved.get('date', '') != today:
                saved.update({'daily_loss': 0.0, 'order_count': 0, 'date': today, 'halted': False})
            risk_state.update(saved)
    except Exception as e:
        logger.warning(f'State load: {e}')

# ── Risk checks ──────────────────────────────────────────────────────────────

def check_risk():
    if risk_state.get('halted'):
        return True, 'Halted by daily loss limit'
    dl = risk_state.get('daily_loss', 0.0)
    if dl <= -abs(MAX_DAILY_LOSS):
        risk_state['halted'] = True
        save_state()
        return True, f'Daily loss limit hit: ${dl:.2f}'
    if risk_state.get('order_count', 0) >= MAX_ORDERS_PER_DAY:
        return True, f'Max orders/day ({MAX_ORDERS_PER_DAY}) reached'
    return False, ''

# ── Underlying price ─────────────────────────────────────────────────────────

async def get_underlying_price():
    """Snapshot the underlying price."""
    ticker = ib.reqMktData(underlying_contract, genericTickList='', snapshot=True,
                           regulatorySnapshot=False)
    await asyncio.sleep(2)
    price = ticker.last or ticker.close or ticker.bid
    if price and not math.isnan(price):
        price_buffer.append(float(price))
        return float(price)
    return None

# ── VIX filter ───────────────────────────────────────────────────────────────

async def get_vix():
    """Fetch current VIX level."""
    try:
        vix = Index('VIX', 'CBOE', 'USD')
        await ib.qualifyContractsAsync(vix)
        ticker = ib.reqMktData(vix, '', snapshot=True, regulatorySnapshot=False)
        await asyncio.sleep(2)
        val = ticker.last or ticker.close
        if val and not math.isnan(val):
            return float(val)
    except Exception as e:
        logger.warning(f'VIX fetch error: {e}')
    return None

async def vix_ok():
    """Check VIX filter. Returns True if filter is disabled or VIX is in range."""
    if not IV_FILTER:
        return True
    vix = await get_vix()
    if vix is None:
        logger.warning('[VIX] Could not fetch — blocking entry as safety measure')
        return False
    ok = MIN_VIX <= vix <= MAX_VIX
    if ok:
        logger.info(f'[VIX] {vix:.2f} within [{MIN_VIX}, {MAX_VIX}] — OK')
    else:
        logger.info(f'[VIX] {vix:.2f} outside [{MIN_VIX}, {MAX_VIX}] — skip')
    return ok

# ── SMA filter ───────────────────────────────────────────────────────────────

def sma_ok(price):
    """Check SMA directional filter. Returns True if disabled or condition met."""
    if not SMA_FILTER:
        return True
    if len(price_buffer) < SMA_PERIOD:
        logger.info(f'[SMA] Only {len(price_buffer)}/{SMA_PERIOD} bars — skipping filter')
        return True
    sma = sum(list(price_buffer)[-SMA_PERIOD:]) / SMA_PERIOD
    if SMA_DIRECTION == 'above':
        ok = price > sma
    else:
        ok = price < sma
    if not ok:
        logger.info(f'[SMA] Price {price:.2f} not {SMA_DIRECTION} SMA({SMA_PERIOD})={sma:.2f}')
    return ok

# ── Strike selection ─────────────────────────────────────────────────────────

async def get_chain_strikes(expiry_str):
    """Get available option strikes from chain parameters."""
    chains = await ib.reqSecDefOptParamsAsync(
        underlying_contract.symbol, '',
        underlying_contract.secType, underlying_contract.conId,
    )
    if not chains:
        return []
    # prefer chain matching our trading class
    chain = None
    for c in chains:
        if OPTION_TRADING_CLASS and OPTION_TRADING_CLASS in (c.tradingClass or ''):
            chain = c
            break
    if chain is None:
        chain = chains[0]
    return sorted(chain.strikes)

def snap_strike(val, strikes):
    """Snap a computed strike value to the nearest available chain strike."""
    if not strikes:
        return round(val / 5) * 5
    return min(strikes, key=lambda s: abs(s - val))

async def find_strike_by_delta(right, target_delta, expiry_str, price, chain_strikes):
    """Select the strike whose delta is closest to target_delta."""
    if right == 'P':
        candidates = [s for s in chain_strikes if s < price][-20:]
    else:
        candidates = [s for s in chain_strikes if s > price][:20]
    if not candidates:
        logger.warning(f'No {right} candidates for delta selection')
        return None

    best_strike, best_diff = None, 999.0
    for strike in candidates:
        opt = Option(UNDERLYING, expiry_str, strike, right, UNDERLYING_EXCHANGE,
                     tradingClass=OPTION_TRADING_CLASS)
        try:
            await ib.qualifyContractsAsync(opt)
            ticker = ib.reqMktData(opt, genericTickList='106', snapshot=True,
                                   regulatorySnapshot=False)
            await asyncio.sleep(1)
            delta = ticker.modelGreeks.delta if ticker.modelGreeks else None
            if delta is not None:
                diff = abs(delta - target_delta)
                if diff < best_diff:
                    best_diff = diff
                    best_strike = strike
                    logger.info(f'  {strike}{right}: delta={delta:.4f} (target={target_delta:.3f}, diff={diff:.4f})')
        except Exception:
            pass
    return best_strike

def find_strike_by_pct(right, pct_otm, price, chain_strikes):
    """Select strike as a percentage OTM from spot."""
    if right == 'P':
        raw = price * (1 - pct_otm)
    else:
        raw = price * (1 + pct_otm)
    return snap_strike(raw, chain_strikes)

def find_strike_by_points(right, points, price, chain_strikes):
    """Select strike as a fixed number of points OTM from spot."""
    if right == 'P':
        raw = price - points
    else:
        raw = price + points
    return snap_strike(raw, chain_strikes)

async def select_strikes(price, expiry_str):
    """Determine short put and/or short call strikes based on strategy type and method."""
    chain_strikes = await get_chain_strikes(expiry_str)
    put_strike = call_strike = None

    needs_put = STRATEGY_TYPE in ('BULL_PUT_SPREAD', 'IRON_CONDOR', 'SHORT_STRANGLE', 'SHORT_STRADDLE')
    needs_call = STRATEGY_TYPE in ('BEAR_CALL_SPREAD', 'IRON_CONDOR', 'SHORT_STRANGLE', 'SHORT_STRADDLE')

    if STRATEGY_TYPE == 'SHORT_STRADDLE':
        atm = snap_strike(price, chain_strikes)
        return atm, atm

    if needs_put:
        if STRIKE_METHOD == 'DELTA':
            put_strike = await find_strike_by_delta('P', PUT_DELTA, expiry_str, price, chain_strikes)
        elif STRIKE_METHOD == 'PCT_OTM':
            put_strike = find_strike_by_pct('P', PUT_PCT_OTM, price, chain_strikes)
        elif STRIKE_METHOD == 'POINTS_OTM':
            put_strike = find_strike_by_points('P', PUT_POINTS_OTM, price, chain_strikes)

    if needs_call:
        if STRIKE_METHOD == 'DELTA':
            call_strike = await find_strike_by_delta('C', CALL_DELTA, expiry_str, price, chain_strikes)
        elif STRIKE_METHOD == 'PCT_OTM':
            call_strike = find_strike_by_pct('C', CALL_PCT_OTM, price, chain_strikes)
        elif STRIKE_METHOD == 'POINTS_OTM':
            call_strike = find_strike_by_points('C', CALL_POINTS_OTM, price, chain_strikes)

    return put_strike, call_strike

# ── Position sizing ──────────────────────────────────────────────────────────

def calc_contracts(wing_width):
    """Size position by max loss per unit vs. risk budget."""
    max_loss_per = wing_width * MULTIPLIER
    if max_loss_per <= 0:
        return MIN_CONTRACTS
    risk_budget = ACCOUNT_EQUITY * RISK_PCT
    n = int(risk_budget / max_loss_per)
    return max(MIN_CONTRACTS, min(MAX_CONTRACTS, n))

# ── BAG combo order builder ──────────────────────────────────────────────────

def build_bag(legs_info):
    """
    Build an IBKR BAG combo contract.
    legs_info: list of {'conId': int, 'action': 'BUY'|'SELL', 'ratio': int}
    """
    bag = Contract()
    bag.symbol   = UNDERLYING
    bag.secType  = 'BAG'
    bag.exchange = 'SMART'
    bag.currency = 'USD'
    bag.comboLegs = []
    for leg in legs_info:
        cl = ComboLeg()
        cl.conId   = leg['conId']
        cl.ratio   = leg.get('ratio', 1)
        cl.action  = leg['action']
        cl.exchange = 'SMART'
        bag.comboLegs.append(cl)
    return bag

# ── Order execution ──────────────────────────────────────────────────────────

async def place_combo_order(bag, action, qty, limit_price=None, signal=''):
    """
    Place a BAG combo order.
    action: 'SELL' for opening credit, 'BUY' for closing debit.
    Returns (fill_price, commission, exec_id).
    """
    if DRY_RUN:
        fake_id = f'DRY_{STRATEGY_NAME}_{datetime.utcnow():%Y%m%d%H%M%S%f}'
        fake_price = abs(limit_price) if limit_price else 0.50
        logger.info(f'[DRY RUN] {action} {qty}x combo | signal={signal} | price={fake_price:.4f}')
        return fake_price, 0.0, fake_id

    halt, reason = check_risk()
    if halt:
        logger.warning(f'Order blocked: {reason}')
        return 0.0, 0.0, ''

    if limit_price is not None:
        order = LimitOrder(action, qty, limit_price)
    else:
        order = MarketOrder(action, qty)
    order.orderRef = STRATEGY_NAME
    order.account  = ACCOUNT_ID
    order.tif      = 'DAY'
    order.smartComboRoutingParams = [TagValue('NonGuaranteed', '1')]

    logger.info(f'>>> {action} {qty}x combo | signal={signal} | limit={limit_price}')
    trade = ib.placeOrder(bag, order)

    loop = asyncio.get_event_loop()
    deadline = loop.time() + ORDER_TIMEOUT
    while loop.time() < deadline:
        await asyncio.sleep(0.5)
        status = trade.orderStatus.status
        if status == 'Filled':
            break
        if status in ('Cancelled', 'Inactive', 'ApiCancelled'):
            logger.error(f'Combo order {status}')
            return 0.0, 0.0, ''

    if trade.orderStatus.status != 'Filled':
        logger.error(f'Combo fill timeout ({ORDER_TIMEOUT}s) — cancelling')
        ib.cancelOrder(trade.order)
        await asyncio.sleep(2)
        return 0.0, 0.0, ''

    fp = float(trade.orderStatus.avgFillPrice or 0.0)
    await asyncio.sleep(2)
    comm = sum(
        float(f.commissionReport.commission)
        for f in trade.fills
        if f.commissionReport and f.commissionReport.commission
        and f.commissionReport.commission > 0
    )
    eid = (trade.fills[0].execution.execId if trade.fills
           else f'{STRATEGY_NAME}_{datetime.utcnow():%Y%m%d%H%M%S%f}')

    risk_state['order_count'] = risk_state.get('order_count', 0) + 1
    risk_state['date'] = now_et().strftime('%Y-%m-%d')
    return fp, comm, eid

# ── Entry logic ──────────────────────────────────────────────────────────────

async def enter_spread(price, expiry_str):
    """Build legs, qualify contracts, size position, and place BAG entry order."""
    global position_open, position_legs, net_credit, entry_time_ts, num_contracts, entry_expiry

    halt, reason = check_risk()
    if halt:
        logger.warning(f'Entry blocked: {reason}')
        return False

    put_strike, call_strike = await select_strikes(price, expiry_str)

    # Validate required strikes
    if STRATEGY_TYPE in ('BULL_PUT_SPREAD', 'IRON_CONDOR') and not put_strike:
        logger.warning('Could not determine put strike — aborting entry')
        return False
    if STRATEGY_TYPE in ('BEAR_CALL_SPREAD', 'IRON_CONDOR') and not call_strike:
        logger.warning('Could not determine call strike — aborting entry')
        return False
    if STRATEGY_TYPE in ('SHORT_STRANGLE', 'SHORT_STRADDLE') and (not put_strike or not call_strike):
        logger.warning('Could not determine both strikes — aborting entry')
        return False

    # Build leg descriptors
    chain_strikes = await get_chain_strikes(expiry_str)
    legs = []

    if STRATEGY_TYPE == 'BULL_PUT_SPREAD':
        put_long = snap_strike(put_strike - PUT_WING_WIDTH, chain_strikes)
        legs = [
            {'strike': put_strike, 'right': 'P', 'action': 'SELL'},
            {'strike': put_long,   'right': 'P', 'action': 'BUY'},
        ]

    elif STRATEGY_TYPE == 'BEAR_CALL_SPREAD':
        call_long = snap_strike(call_strike + CALL_WING_WIDTH, chain_strikes)
        legs = [
            {'strike': call_strike, 'right': 'C', 'action': 'SELL'},
            {'strike': call_long,   'right': 'C', 'action': 'BUY'},
        ]

    elif STRATEGY_TYPE == 'IRON_CONDOR':
        put_long  = snap_strike(put_strike  - PUT_WING_WIDTH,  chain_strikes)
        call_long = snap_strike(call_strike + CALL_WING_WIDTH, chain_strikes)
        legs = [
            {'strike': put_long,    'right': 'P', 'action': 'BUY'},
            {'strike': put_strike,  'right': 'P', 'action': 'SELL'},
            {'strike': call_strike, 'right': 'C', 'action': 'SELL'},
            {'strike': call_long,   'right': 'C', 'action': 'BUY'},
        ]

    elif STRATEGY_TYPE == 'SHORT_STRANGLE':
        legs = [
            {'strike': put_strike,  'right': 'P', 'action': 'SELL'},
            {'strike': call_strike, 'right': 'C', 'action': 'SELL'},
        ]

    elif STRATEGY_TYPE == 'SHORT_STRADDLE':
        legs = [
            {'strike': put_strike,  'right': 'P', 'action': 'SELL'},
            {'strike': call_strike, 'right': 'C', 'action': 'SELL'},
        ]

    if not legs:
        logger.error(f'No legs built for {STRATEGY_TYPE}')
        return False

    strikes_desc = ' / '.join(f'{l["action"]} {l["strike"]}{l["right"]}' for l in legs)
    logger.info(f'[ENTRY] {STRATEGY_TYPE} | spot={price:.2f} | {strikes_desc}')

    # Qualify each option leg
    for leg in legs:
        opt = Option(UNDERLYING, expiry_str, leg['strike'], leg['right'],
                     UNDERLYING_EXCHANGE, tradingClass=OPTION_TRADING_CLASS)
        try:
            qualified = await ib.qualifyContractsAsync(opt)
            if qualified and qualified[0].conId > 0:
                leg['conId'] = qualified[0].conId
                leg['localSymbol'] = qualified[0].localSymbol
                logger.info(f'  Qualified: {qualified[0].localSymbol} conId={qualified[0].conId}')
            else:
                logger.error(f'  Failed to qualify: {leg["strike"]}{leg["right"]}')
                return False
        except Exception as e:
            logger.error(f'  Qualification error {leg["strike"]}{leg["right"]}: {e}')
            return False

    # Position sizing
    if STRATEGY_TYPE in ('BULL_PUT_SPREAD', 'BEAR_CALL_SPREAD'):
        wing = PUT_WING_WIDTH if STRATEGY_TYPE == 'BULL_PUT_SPREAD' else CALL_WING_WIDTH
        contracts = calc_contracts(wing)
    elif STRATEGY_TYPE == 'IRON_CONDOR':
        wing = max(PUT_WING_WIDTH, CALL_WING_WIDTH)
        contracts = calc_contracts(wing)
    else:
        # naked strategies — use conservative sizing based on notional
        notional_risk = price * 0.10 * MULTIPLIER
        contracts = max(MIN_CONTRACTS, min(MAX_CONTRACTS,
                        int(ACCOUNT_EQUITY * RISK_PCT / notional_risk) if notional_risk > 0 else MIN_CONTRACTS))
    logger.info(f'  Contracts: {contracts}')

    # Build BAG and place entry order
    bag_legs = [{'conId': l['conId'], 'action': l['action'], 'ratio': 1} for l in legs]
    bag = build_bag(bag_legs)

    fp, comm, eid = await place_combo_order(bag, 'SELL', contracts, signal='entry')
    if not eid:
        logger.error('Entry combo order failed or was not filled')
        return False

    credit = abs(fp)
    logger.info(f'[FILLED] {contracts}x @ net credit {credit:.4f} | commission=${comm:.2f}')

    # Update global state
    position_open  = True
    position_legs  = legs
    net_credit     = credit
    entry_time_ts  = datetime.utcnow().isoformat()
    num_contracts  = contracts
    entry_expiry   = expiry_str

    risk_state['date'] = now_et().strftime('%Y-%m-%d')

    log_trade('SLD', contracts, credit, comm, eid, 0.0, 'entry')
    save_state()
    return True

# ── Exit logic ───────────────────────────────────────────────────────────────

async def close_position(signal='exit'):
    """Close the open position by buying back the combo."""
    global position_open, position_legs, net_credit, entry_time_ts, num_contracts, entry_expiry

    if not position_open or not position_legs:
        return False

    # Reverse actions for closing
    reversed_legs = []
    for leg in position_legs:
        rev_action = 'BUY' if leg['action'] == 'SELL' else 'SELL'
        reversed_legs.append({'conId': leg['conId'], 'action': rev_action, 'ratio': 1})

    bag = build_bag(reversed_legs)

    fp, comm, eid = await place_combo_order(bag, 'BUY', num_contracts, signal=signal)
    if not eid:
        logger.error(f'Close order failed ({signal})')
        return False

    debit = abs(fp)
    pnl = (net_credit - debit) * num_contracts * MULTIPLIER - comm
    logger.info(f'[EXIT] {signal} | debit={debit:.4f} credit={net_credit:.4f} | pnl=${pnl:+.2f}')

    risk_state['daily_loss']  = risk_state.get('daily_loss', 0.0) + pnl
    risk_state['session_pnl'] = risk_state.get('session_pnl', 0.0) + pnl

    log_trade('BOT', num_contracts, debit, comm, eid, pnl, signal)

    position_open  = False
    position_legs  = []
    net_credit     = 0.0
    entry_time_ts  = ''
    num_contracts  = 0
    entry_expiry   = ''

    save_state()
    return True

# ── P&L monitoring ───────────────────────────────────────────────────────────

async def get_spread_mid():
    """Get current mid-price of the open combo for P&L monitoring."""
    if not position_legs:
        return None
    total = 0.0
    for leg in position_legs:
        opt = Option(conId=leg['conId'])
        try:
            await ib.qualifyContractsAsync(opt)
        except Exception:
            pass
        ticker = ib.reqMktData(opt, '', snapshot=True, regulatorySnapshot=False)
        await asyncio.sleep(1)
        bid = ticker.bid if ticker.bid and not math.isnan(ticker.bid) and ticker.bid > 0 else 0
        ask = ticker.ask if ticker.ask and not math.isnan(ticker.ask) and ticker.ask > 0 else 0
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else (bid or ask or 0)
        # SELL legs add to cost-to-close; BUY legs subtract
        if leg['action'] == 'SELL':
            total += mid
        else:
            total -= mid
    return total  # positive = cost to close the position

async def check_exit_conditions():
    """Evaluate profit target, loss limit, and EOD exit."""
    if not position_open:
        return

    cost_to_close = await get_spread_mid()
    if cost_to_close is None:
        logger.warning('[MONITOR] Could not price spread')
        return

    unrealised = (net_credit - cost_to_close) * num_contracts * MULTIPLIER
    logger.info(f'[MONITOR] cost_to_close={cost_to_close:.4f} | credit={net_credit:.4f} | unrealised=${unrealised:+.2f}')

    # Profit target: captured enough of the credit
    if PROFIT_TARGET_PCT > 0 and net_credit > 0:
        target = net_credit * (1 - PROFIT_TARGET_PCT)
        if cost_to_close <= target:
            logger.info(f'[PROFIT TARGET] {PROFIT_TARGET_PCT*100:.0f}% of credit captured')
            await close_position(signal='profit_target')
            return

    # Loss limit: cost to close exceeds credit by multiplier
    if LOSS_LIMIT_MULT > 0 and net_credit > 0:
        max_cost = net_credit * LOSS_LIMIT_MULT
        if cost_to_close >= max_cost:
            logger.info(f'[LOSS LIMIT] cost {cost_to_close:.4f} >= {max_cost:.4f} ({LOSS_LIMIT_MULT}x credit)')
            await close_position(signal='loss_limit')
            return

    # EOD forced exit
    if EOD_EXIT and past_exit_time():
        logger.info(f'[EOD EXIT] Past {EXIT_TIME_ET} ET — closing')
        await close_position(signal='eod_exit')
        return

# ── Greeks snapshot ──────────────────────────────────────────────────────────

async def get_greeks_snapshot():
    """Fetch greeks for all open legs — used by heartbeat."""
    if not position_legs:
        return {}
    snapshot = {}
    for leg in position_legs:
        key = f'{leg.get("strike","")}{leg.get("right","")}'
        try:
            opt = Option(conId=leg['conId'])
            await ib.qualifyContractsAsync(opt)
            ticker = ib.reqMktData(opt, genericTickList='106', snapshot=True,
                                   regulatorySnapshot=False)
            await asyncio.sleep(1)
            g = ticker.modelGreeks
            if g:
                snapshot[key] = {
                    'delta': round(g.delta, 4) if g.delta else 0,
                    'gamma': round(g.gamma, 6) if g.gamma else 0,
                    'theta': round(g.theta, 4) if g.theta else 0,
                    'vega':  round(g.vega, 4)  if g.vega  else 0,
                    'iv':    round(g.impliedVol, 4) if g.impliedVol else 0,
                }
        except Exception as e:
            logger.warning(f'Greeks error for {key}: {e}')
    return snapshot

# ── Main loops ───────────────────────────────────────────────────────────────

async def entry_loop():
    """Attempt entry once per day during the entry window."""
    today_entered = False
    last_date = ''
    while True:
        try:
            await asyncio.sleep(MONITOR_INTERVAL)
            today_str = now_et().strftime('%Y-%m-%d')

            # Reset daily flag at midnight
            if today_str != last_date:
                today_entered = False
                last_date = today_str

            if today_entered or position_open:
                continue
            if not in_trade_window():
                continue

            halt, reason = check_risk()
            if halt:
                continue

            price = await get_underlying_price()
            if not price:
                logger.warning('No underlying price — retry next cycle')
                continue

            logger.info(f'[SIGNAL] {UNDERLYING}={price:.2f} | SMA_OK={sma_ok(price)}')

            if not sma_ok(price):
                continue
            if not await vix_ok():
                continue
            if is_fomc_day():
                logger.info('[FOMC] Skipping entry on FOMC day')
                continue

            expiry_str = now_et().strftime('%Y%m%d')
            success = await enter_spread(price, expiry_str)
            if success:
                today_entered = True

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f'Entry loop error: {e}')
            await asyncio.sleep(10)

async def monitor_loop():
    """Monitor open position for profit target, loss limit, and EOD exit."""
    while True:
        try:
            await asyncio.sleep(MONITOR_INTERVAL)
            if not position_open:
                continue
            if not ib.isConnected():
                logger.warning('[MONITOR] Disconnected — skipping')
                continue
            await check_exit_conditions()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f'Monitor loop error: {e}')
            await asyncio.sleep(10)

async def heartbeat_loop():
    """Periodic heartbeat with greeks snapshot."""
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_SECS)
            t = now_et().strftime('%H:%M ET')
            parts = [f'[HB] {t}']
            parts.append(f'pos={"OPEN" if position_open else "FLAT"}')
            parts.append(f'daily_loss=${risk_state.get("daily_loss", 0):.2f}')
            parts.append(f'session=${risk_state.get("session_pnl", 0):.2f}')
            parts.append(f'orders={risk_state.get("order_count", 0)}')

            if position_open:
                parts.append(f'contracts={num_contracts}')
                parts.append(f'credit={net_credit:.4f}')
                parts.append(f'legs={len(position_legs)}')

                greeks = await get_greeks_snapshot()
                if greeks:
                    net_delta = 0.0
                    net_theta = 0.0
                    for key, g in greeks.items():
                        # short legs contribute negative delta/vega, positive theta
                        is_short = any(
                            str(l.get('strike', '')) in key and l.get('right', '') in key
                            and l['action'] == 'SELL'
                            for l in position_legs
                        )
                        sign = -1 if is_short else 1
                        net_delta += g.get('delta', 0) * sign * num_contracts * MULTIPLIER
                        net_theta += g.get('theta', 0) * sign * num_contracts * MULTIPLIER
                    parts.append(f'net_delta={net_delta:+.2f}')
                    parts.append(f'net_theta={net_theta:+.2f}')
                    parts.append(f'greeks={json.dumps(greeks)}')

            logger.info(' | '.join(parts))

        except asyncio.CancelledError:
            break
        except Exception:
            pass

async def daily_reset_loop():
    """Reset daily risk counters at RESTART_TIME_ET each day."""
    last_reset_date = ''
    while True:
        try:
            await asyncio.sleep(30)
            now = now_et()
            today_str = now.strftime('%Y-%m-%d')
            if now.time() >= parse_time(RESTART_TIME_ET) and today_str != last_reset_date:
                logger.info(f'[RESET] Daily risk counters at {RESTART_TIME_ET} ET')
                risk_state['daily_loss'] = 0.0
                risk_state['order_count'] = 0
                risk_state['halted'] = False
                risk_state['date'] = today_str
                last_reset_date = today_str
                save_state()
        except asyncio.CancelledError:
            break
        except Exception:
            pass

# ── Connection ───────────────────────────────────────────────────────────────

async def connect_retry():
    for attempt in range(1, 6):
        try:
            await ib.connectAsync('127.0.0.1', PORT, CLIENT_ID, timeout=15)
            logger.info(f'Connected to IBKR [attempt {attempt}]')
            return True
        except Exception as e:
            wait = min(2 ** attempt, 30)
            logger.warning(f'Connect attempt {attempt}: {e}. Retry in {wait}s...')
            await asyncio.sleep(wait)
    return False

# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    global underlying_contract

    logger.info('=' * 72)
    logger.info(f'{STRATEGY_NAME} | {UNDERLYING} ({UNDERLYING_SEC_TYPE}) | {STRATEGY_TYPE}')
    logger.info(f'Strike: {STRIKE_METHOD} | Put delta={PUT_DELTA} / pct={PUT_PCT_OTM} / pts={PUT_POINTS_OTM}')
    logger.info(f'                        | Call delta={CALL_DELTA} / pct={CALL_PCT_OTM} / pts={CALL_POINTS_OTM}')
    logger.info(f'Wings: put={PUT_WING_WIDTH} call={CALL_WING_WIDTH} | Multiplier={MULTIPLIER}')
    logger.info(f'Window: {ENTRY_TIME_ET}–{EXIT_TIME_ET} ET | Restart: {RESTART_TIME_ET} ET')
    logger.info(f'Profit target: {PROFIT_TARGET_PCT*100:.0f}% | Loss limit: {LOSS_LIMIT_MULT}x | EOD exit: {EOD_EXIT}')
    logger.info(f'Risk: {RISK_PCT*100:.1f}% of ${ACCOUNT_EQUITY:,.0f} | Contracts [{MIN_CONTRACTS}–{MAX_CONTRACTS}]')
    logger.info(f'Daily loss limit: ${MAX_DAILY_LOSS:,.0f} | Max orders/day: {MAX_ORDERS_PER_DAY} | DRY_RUN: {DRY_RUN}')
    if SMA_FILTER:
        logger.info(f'SMA filter: price {SMA_DIRECTION} SMA({SMA_PERIOD})')
    if IV_FILTER:
        logger.info(f'VIX filter: [{MIN_VIX}, {MAX_VIX}]')
    if SKIP_FOMC:
        logger.info(f'FOMC skip: {len(FOMC_DATES)} dates')
    logger.info('=' * 72)

    load_state()

    if not await connect_retry():
        logger.error('IBKR connection failed after all attempts')
        return

    # Build and qualify underlying
    if UNDERLYING_SEC_TYPE == 'IND':
        underlying_contract = Index(UNDERLYING, UNDERLYING_EXCHANGE, 'USD')
    elif UNDERLYING_SEC_TYPE == 'FUT':
        underlying_contract = ContFuture(UNDERLYING, exchange=UNDERLYING_EXCHANGE, currency='USD')
    else:
        underlying_contract = Stock(UNDERLYING, 'SMART', 'USD')

    try:
        q = await ib.qualifyContractsAsync(underlying_contract)
        if q:
            underlying_contract = q[0]
            logger.info(f'Underlying: {underlying_contract.symbol} conId={underlying_contract.conId}')
        else:
            logger.error('Underlying qualification failed')
            ib.disconnect()
            return
    except Exception as e:
        logger.error(f'Underlying contract error: {e}')
        ib.disconnect()
        return

    # Warm up SMA buffer
    if SMA_FILTER:
        try:
            bars = await ib.reqHistoricalDataAsync(
                underlying_contract, endDateTime='', durationStr='60 D',
                barSizeSetting='1 day', whatToShow='TRADES', useRTH=True, formatDate=1,
            )
            for b in bars:
                price_buffer.append(float(b.close))
            logger.info(f'SMA warmup: {len(bars)} daily bars')
        except Exception as e:
            logger.warning(f'SMA warmup failed: {e}')

    price = await get_underlying_price()
    logger.info(f'{UNDERLYING} current price: {price}')
    ensure_csv()

    if position_open:
        logger.warning(f'[RESTORED] Open position: {num_contracts}x | credit={net_credit:.4f} | '
                       f'legs={len(position_legs)} | expiry={entry_expiry}')

    try:
        await asyncio.gather(
            entry_loop(),
            monitor_loop(),
            heartbeat_loop(),
            daily_reset_loop(),
        )
    except Exception as e:
        logger.exception(f'Fatal error: {e}')
    finally:
        save_state()
        if ib.isConnected():
            ib.disconnect()
        logger.info('Shutdown complete.')

if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Ctrl+C — shutting down')
        save_state()
        if ib.isConnected():
            ib.disconnect()
'''
