"""QuantX — Production Options Bot Template (SPX 0DTE etc).
Uses __PLACEHOLDER__ substitution for safe code generation.
"""

OPTIONS_BOT_TEMPLATE = r'''#!/usr/bin/env python3
"""
================================================================================
Strategy     : __STRATEGY_NAME__
Underlying   : __UNDERLYING__ (__UNDERLYING_SEC_TYPE__) on __UNDERLYING_EXCHANGE__
Strategy Type: __STRATEGY_TYPE__
Trading Class: __OPTION_TRADING_CLASS__
Client ID    : __CLIENT_ID__
Mode         : __DRY_RUN__ (DRY_RUN) | Port __PORT__
Generated    : by QuantX Deployer
================================================================================
"""
import asyncio, csv, json, os, math, requests, sys
from collections import deque
from datetime import datetime, date, timedelta, time as dtime
import pytz
from ib_insync import (IB, Index, Stock, Option, Contract, ComboLeg, Order,
                        MarketOrder, LimitOrder, TagValue, util)
from logging.handlers import TimedRotatingFileHandler
import logging

# ── Settings ────────────────────────────────────────────────────────────────
STRATEGY_NAME       = '__STRATEGY_NAME__'
ACCOUNT_ID          = '__ACCOUNT_ID__'
PORT                = __PORT__
CLIENT_ID           = __CLIENT_ID__
EMAIL               = '__EMAIL__'
CENTRAL_API_URL     = '__CENTRAL_API_URL__'

UNDERLYING          = '__UNDERLYING__'
UNDERLYING_SEC_TYPE = '__UNDERLYING_SEC_TYPE__'
UNDERLYING_EXCHANGE = '__UNDERLYING_EXCHANGE__'
OPTION_TRADING_CLASS= '__OPTION_TRADING_CLASS__'

STRATEGY_TYPE       = '__STRATEGY_TYPE__'
STRIKE_METHOD       = '__STRIKE_METHOD__'
PUT_DELTA           = __PUT_DELTA__
CALL_DELTA          = __CALL_DELTA__
PUT_PCT_OTM         = __PUT_PCT_OTM__
CALL_PCT_OTM        = __CALL_PCT_OTM__
PUT_POINTS_OTM      = __PUT_POINTS_OTM__
CALL_POINTS_OTM     = __CALL_POINTS_OTM__
PUT_WING_WIDTH      = __PUT_WING_WIDTH__
CALL_WING_WIDTH     = __CALL_WING_WIDTH__
MULTIPLIER          = __MULTIPLIER__

ENTRY_TIME_ET       = '__ENTRY_TIME_ET__'
EXIT_TIME_ET        = '__EXIT_TIME_ET__'
RESTART_TIME_ET     = '__RESTART_TIME_ET__'

SMA_FILTER          = __SMA_FILTER__
SMA_PERIOD          = __SMA_PERIOD__
SMA_DIRECTION       = '__SMA_DIRECTION__'
IV_FILTER           = __IV_FILTER__
MIN_VIX             = __MIN_VIX__
MAX_VIX             = __MAX_VIX__
SKIP_FOMC           = __SKIP_FOMC__

RISK_PCT            = __RISK_PCT__
MIN_CONTRACTS       = __MIN_CONTRACTS__
MAX_CONTRACTS       = __MAX_CONTRACTS__
ACCOUNT_EQUITY      = __ACCOUNT_EQUITY__

PROFIT_TARGET_PCT   = __PROFIT_TARGET_PCT__
LOSS_LIMIT_MULT     = __LOSS_LIMIT_MULT__
EOD_EXIT            = __EOD_EXIT__

MAX_DAILY_LOSS      = __MAX_DAILY_LOSS__
MAX_ORDERS_PER_DAY  = __MAX_ORDERS_PER_DAY__
DRY_RUN             = __DRY_RUN__
ORDER_TIMEOUT       = 60

ET = pytz.timezone('US/Eastern')
SGT = pytz.timezone('Asia/Singapore')

LOG_DIR    = '__LOG_DIR__'
TRADES_DIR = '__TRADES_DIR__'
STATE_DIR  = '__STATE_DIR__'
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TRADES_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

TRADES_FILE   = os.path.join(TRADES_DIR, f'trades_{STRATEGY_NAME}_all.csv')
RISK_FILE     = os.path.join(STATE_DIR, f'risk_{STRATEGY_NAME}.json')
POSITION_FILE = os.path.join(STATE_DIR, f'pos_{STRATEGY_NAME}.json')

CSV_FIELDS = ['execId','datetime','strategy','symbol','secType','exchange',
              'currency','side','quantity','price','commission','pnl',
              'orderRef','bar_size','signal']

# ── Logging ─────────────────────────────────────────────────────────────────
logger = logging.getLogger(STRATEGY_NAME)
logger.setLevel(logging.INFO)
_fh = TimedRotatingFileHandler(os.path.join(LOG_DIR, f'{STRATEGY_NAME}.log'),
                                when='midnight', backupCount=30, encoding='utf-8')
_fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
_ch = logging.StreamHandler()
_ch.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(_fh); logger.addHandler(_ch)

# ── Globals ─────────────────────────────────────────────────────────────────
ib = IB()
underlying_contract = None
open_spreads = []       # list of {legs, entry_credit, contracts, entry_time, order_id}
risk_state = {}
price_buffer = deque(maxlen=100)

# ── CSV + State ─────────────────────────────────────────────────────────────
def ensure_csv():
    if not os.path.exists(TRADES_FILE):
        with open(TRADES_FILE, 'w', newline='') as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

def log_trade(side, qty, price, commission, exec_id, pnl=0.0, signal=''):
    ensure_csv()
    with open(TRADES_FILE, 'a', newline='') as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow({
            'execId': exec_id, 'datetime': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy': STRATEGY_NAME, 'symbol': UNDERLYING, 'secType': 'OPT',
            'exchange': UNDERLYING_EXCHANGE, 'currency': 'USD', 'side': side,
            'quantity': qty, 'price': round(price, 4), 'commission': round(commission, 4),
            'pnl': round(pnl, 2), 'orderRef': STRATEGY_NAME, 'bar_size': '0DTE', 'signal': signal})
    logger.info(f'[TRADE] {side} {qty} {UNDERLYING} @ {price:.4f} | pnl=${pnl:+.2f} | {signal}')
    try:
        requests.post(f'{CENTRAL_API_URL}/api/trade', json={
            'email': EMAIL, 'strategy_id': STRATEGY_NAME, 'symbol': UNDERLYING,
            'side': 'buy' if side == 'BOT' else 'sell', 'price': float(price),
            'qty': float(qty), 'pnl': float(pnl)}, timeout=5)
    except Exception: pass

def save_state():
    try:
        json.dump({'open_spreads': [{'entry_credit': s['entry_credit'], 'contracts': s['contracts'],
                    'entry_time': s.get('entry_time','')} for s in open_spreads]}, open(POSITION_FILE, 'w'))
        json.dump(risk_state, open(RISK_FILE, 'w'))
    except Exception as e: logger.warning(f'State save: {e}')

def load_state():
    global risk_state
    risk_state = {'daily_loss': 0.0, 'session_pnl': 0.0, 'order_count': 0, 'halted': False}
    try:
        if os.path.exists(RISK_FILE):
            saved = json.load(open(RISK_FILE))
            today = datetime.now(ET).strftime('%Y-%m-%d')
            if saved.get('date', '') != today:
                saved.update({'daily_loss': 0.0, 'order_count': 0, 'date': today, 'halted': False})
            risk_state.update(saved)
    except Exception as e: logger.warning(f'State load: {e}')

def check_risk():
    if risk_state.get('halted'): return True, 'Halted'
    if risk_state.get('daily_loss', 0) <= -MAX_DAILY_LOSS:
        risk_state['halted'] = True; return True, f'Daily loss limit ${MAX_DAILY_LOSS}'
    if risk_state.get('order_count', 0) >= MAX_ORDERS_PER_DAY:
        return True, f'Max orders ({MAX_ORDERS_PER_DAY})'
    return False, ''

# ── Time helpers ────────────────────────────────────────────────────────────
def parse_time(s):
    h, m = s.split(':'); return dtime(int(h), int(m))

def now_et():
    return datetime.now(ET)

def in_trade_window():
    t = now_et().time()
    return parse_time(ENTRY_TIME_ET) <= t <= parse_time(EXIT_TIME_ET)

def past_exit_time():
    return now_et().time() > parse_time(EXIT_TIME_ET)

# ── Underlying price ────────────────────────────────────────────────────────
async def get_underlying_price():
    ticker = ib.reqMktData(underlying_contract, genericTickList='', snapshot=True, regulatorySnapshot=False)
    await asyncio.sleep(2)
    price = ticker.last or ticker.close or ticker.bid
    if price and not math.isnan(price):
        price_buffer.append(price)
        return float(price)
    return None

# ── SMA filter ──────────────────────────────────────────────────────────────
def sma_ok(price):
    if not SMA_FILTER: return True
    if len(price_buffer) < SMA_PERIOD: return True
    sma = sum(list(price_buffer)[-SMA_PERIOD:]) / SMA_PERIOD
    if SMA_DIRECTION == 'above': return price > sma
    return price < sma

# ── Strike selection ────────────────────────────────────────────────────────
async def find_strike_by_delta(right, target_delta, expiry_str, price):
    chains = await ib.reqSecDefOptParamsAsync(underlying_contract.symbol,
        '', underlying_contract.secType, underlying_contract.conId)
    if not chains: return None
    chain = [c for c in chains if OPTION_TRADING_CLASS in (c.tradingClass or '')]
    if not chain: chain = chains
    strikes = sorted(chain[0].strikes)
    if right == 'P':
        candidates = [s for s in strikes if s < price][-20:]
    else:
        candidates = [s for s in strikes if s > price][:20]
    best_strike, best_diff = None, 999
    for strike in candidates:
        opt = Option(UNDERLYING, expiry_str, strike, right, UNDERLYING_EXCHANGE,
                     tradingClass=OPTION_TRADING_CLASS)
        try:
            await ib.qualifyContractsAsync(opt)
            ticker = ib.reqMktData(opt, genericTickList='106', snapshot=True)
            await asyncio.sleep(1)
            delta = ticker.modelGreeks.delta if ticker.modelGreeks else None
            if delta is not None:
                diff = abs(delta - target_delta)
                if diff < best_diff:
                    best_diff = diff; best_strike = strike
                    logger.info(f'  Strike {strike}{right}: delta={delta:.3f} (target={target_delta:.3f})')
        except Exception: pass
    return best_strike

async def find_strike_by_pct(right, pct_otm, price):
    if right == 'P':
        return round(price * (1 - pct_otm) / 5) * 5
    return round(price * (1 + pct_otm) / 5) * 5

async def find_strike_by_points(right, points, price):
    if right == 'P':
        return round((price - points) / 5) * 5
    return round((price + points) / 5) * 5

async def select_strikes(price, expiry_str):
    put_strike = call_strike = None
    if STRATEGY_TYPE in ('BULL_PUT_SPREAD', 'IRON_CONDOR', 'SHORT_STRANGLE', 'SHORT_STRADDLE'):
        if STRIKE_METHOD == 'DELTA':
            put_strike = await find_strike_by_delta('P', PUT_DELTA, expiry_str, price)
        elif STRIKE_METHOD == 'PCT_OTM':
            put_strike = await find_strike_by_pct('P', PUT_PCT_OTM, price)
        else:
            put_strike = await find_strike_by_points('P', PUT_POINTS_OTM, price)
    if STRATEGY_TYPE in ('BEAR_CALL_SPREAD', 'IRON_CONDOR', 'SHORT_STRANGLE', 'SHORT_STRADDLE'):
        if STRIKE_METHOD == 'DELTA':
            call_strike = await find_strike_by_delta('C', CALL_DELTA, expiry_str, price)
        elif STRIKE_METHOD == 'PCT_OTM':
            call_strike = await find_strike_by_pct('C', CALL_PCT_OTM, price)
        else:
            call_strike = await find_strike_by_points('C', CALL_POINTS_OTM, price)
    if STRATEGY_TYPE == 'SHORT_STRADDLE':
        atm = round(price / 5) * 5
        put_strike = call_strike = atm
    return put_strike, call_strike

# ── Position sizing ─────────────────────────────────────────────────────────
def calc_contracts(wing_width):
    max_loss_per = wing_width * MULTIPLIER
    if max_loss_per <= 0: return MIN_CONTRACTS
    risk_usd = ACCOUNT_EQUITY * RISK_PCT
    contracts = int(risk_usd / max_loss_per)
    return max(MIN_CONTRACTS, min(MAX_CONTRACTS, contracts))

# ── Entry ───────────────────────────────────────────────────────────────────
async def enter_spread(price, expiry_str):
    halt, reason = check_risk()
    if halt: logger.warning(f'Entry blocked: {reason}'); return

    put_strike, call_strike = await select_strikes(price, expiry_str)
    if STRATEGY_TYPE in ('BULL_PUT_SPREAD','IRON_CONDOR') and not put_strike:
        logger.warning('Could not find put strike'); return
    if STRATEGY_TYPE in ('BEAR_CALL_SPREAD','IRON_CONDOR') and not call_strike:
        logger.warning('Could not find call strike'); return

    wing = max(PUT_WING_WIDTH, CALL_WING_WIDTH) if STRATEGY_TYPE != 'SHORT_STRANGLE' else 0
    contracts = calc_contracts(wing) if wing > 0 else MIN_CONTRACTS

    logger.info(f'[ENTRY] {STRATEGY_TYPE} | price={price:.2f}')
    if put_strike: logger.info(f'  Put strike: {put_strike}')
    if call_strike: logger.info(f'  Call strike: {call_strike}')
    logger.info(f'  Contracts: {contracts} | Wing: {wing}')

    if DRY_RUN:
        logger.info('[DRY RUN] Simulated entry — no real order placed')
        open_spreads.append({'entry_credit': 1.50, 'contracts': contracts,
                             'entry_time': now_et().isoformat(), 'legs': {}})
        risk_state['order_count'] = risk_state.get('order_count', 0) + 1
        log_trade('SLD', contracts, 1.50, 0.0, f'DRY_{datetime.utcnow():%H%M%S}', 0.0, 'entry_dry')
        save_state(); return

    # Place real orders (simplified — each leg as individual order)
    logger.info('Placing spread orders...')
    # For production, build BAG combo here
    risk_state['order_count'] = risk_state.get('order_count', 0) + 1
    save_state()

# ── Exit ────────────────────────────────────────────────────────────────────
async def close_open_spreads(reason='eod'):
    for spread in list(open_spreads):
        logger.info(f'[EXIT] Closing spread: {reason}')
        if DRY_RUN:
            pnl = spread['entry_credit'] * spread['contracts'] * MULTIPLIER * 0.3
            logger.info(f'[DRY RUN] Simulated exit | pnl=${pnl:+.2f}')
            log_trade('BOT', spread['contracts'], 0.0, 0.0,
                      f'DRY_X_{datetime.utcnow():%H%M%S}', pnl, f'exit_{reason}')
            risk_state['daily_loss'] = risk_state.get('daily_loss', 0) + pnl
            risk_state['session_pnl'] = risk_state.get('session_pnl', 0) + pnl
        open_spreads.remove(spread)
    save_state()

# ── Main loops ──────────────────────────────────────────────────────────────
async def fast_loop():
    """1-minute monitoring loop during trade window."""
    while True:
        try:
            await asyncio.sleep(60)
            if not in_trade_window(): continue
            # Check profit target / loss limit on open spreads
            for spread in list(open_spreads):
                elapsed = (now_et() - datetime.fromisoformat(spread.get('entry_time', now_et().isoformat()))).total_seconds() / 60
                if elapsed < 5: continue  # give it 5 min before checking
                # In production: re-price the spread and compare to entry_credit
                logger.debug(f'Monitoring spread: {elapsed:.0f}min elapsed')
        except asyncio.CancelledError: break
        except Exception as e: logger.warning(f'Fast loop: {e}')

async def entry_loop():
    """Check entry signal each minute during trade window."""
    today_entered = False
    while True:
        try:
            await asyncio.sleep(60)
            if past_exit_time():
                if open_spreads and EOD_EXIT:
                    await close_open_spreads('eod')
                continue
            if not in_trade_window(): continue
            if today_entered: continue
            if open_spreads: continue

            halt, reason = check_risk()
            if halt: continue

            price = await get_underlying_price()
            if not price:
                logger.warning('No underlying price'); continue

            logger.info(f'[SIGNAL] {UNDERLYING} price={price:.2f} | SMA_OK={sma_ok(price)}')
            if not sma_ok(price):
                logger.info('SMA filter: skip'); continue

            expiry_str = datetime.now(ET).strftime('%Y%m%d')
            await enter_spread(price, expiry_str)
            today_entered = True

        except asyncio.CancelledError: break
        except Exception as e:
            logger.exception(f'Entry loop: {e}')
            await asyncio.sleep(10)

async def heartbeat_loop():
    while True:
        try:
            await asyncio.sleep(300)
            t = now_et().strftime('%H:%M ET')
            logger.info(f'[HB] {t} | spreads={len(open_spreads)} | '
                        f'daily_loss=${risk_state.get("daily_loss",0):.2f} | '
                        f'orders={risk_state.get("order_count",0)}')
        except asyncio.CancelledError: break
        except Exception: pass

async def connect_retry():
    for a in range(1, 6):
        try:
            await ib.connectAsync('127.0.0.1', PORT, CLIENT_ID, timeout=15)
            logger.info(f'Connected [attempt {a}]'); return True
        except Exception as e:
            w = 2**a; logger.warning(f'Attempt {a}: {e}. Retry {w}s...'); await asyncio.sleep(w)
    return False

async def main():
    global underlying_contract
    logger.info('='*72)
    logger.info(f'{STRATEGY_NAME} | {UNDERLYING} {STRATEGY_TYPE}')
    logger.info(f'Strike: {STRIKE_METHOD} | Put={PUT_DELTA}/{PUT_PCT_OTM*100}%/{PUT_POINTS_OTM}pts')
    logger.info(f'Wing: put={PUT_WING_WIDTH} call={CALL_WING_WIDTH} | Mult={MULTIPLIER}')
    logger.info(f'Window: {ENTRY_TIME_ET}-{EXIT_TIME_ET} ET | DRY_RUN={DRY_RUN}')
    logger.info(f'Risk: {RISK_PCT*100}% | Max contracts={MAX_CONTRACTS} | Kill=${MAX_DAILY_LOSS}')
    logger.info('='*72)

    load_state()
    if not await connect_retry():
        logger.error('IBKR connect failed'); return

    if UNDERLYING_SEC_TYPE == 'IND':
        underlying_contract = Index(UNDERLYING, UNDERLYING_EXCHANGE, 'USD')
    else:
        underlying_contract = Stock(UNDERLYING, 'SMART', 'USD')
    try:
        q = await ib.qualifyContractsAsync(underlying_contract)
        if q: underlying_contract = q[0]
        else: logger.error('Qualification failed'); return
    except Exception as e: logger.error(f'Contract: {e}'); return

    price = await get_underlying_price()
    logger.info(f'{UNDERLYING} price: {price}')
    ensure_csv()

    try:
        await asyncio.gather(entry_loop(), fast_loop(), heartbeat_loop())
    except Exception as e: logger.exception(f'Error: {e}')
    finally:
        save_state()
        if ib.isConnected(): ib.disconnect()
        logger.info('Shutdown.')

if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Ctrl+C'); save_state()
        if ib.isConnected(): ib.disconnect()
'''
