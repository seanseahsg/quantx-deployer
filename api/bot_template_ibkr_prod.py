"""QuantX — Production IBKR bot template.
Uses __PLACEHOLDER__ substitution for safe code generation.
"""

IBKR_PROD_TEMPLATE = r'''#!/usr/bin/env python3
"""
================================================================================
Strategy  : __STRATEGY_NAME__
Symbol    : __SYMBOL__ (__SEC_TYPE__)
Exchange  : __EXCHANGE__
Client ID : __CLIENT_ID__
Generated : by QuantX Deployer
================================================================================
"""
import asyncio, csv, json, os, math, requests, sys
from collections import deque
from datetime import datetime, timedelta
import pytz
from ib_insync import IB, Stock, Forex, ContFuture, MarketOrder, LimitOrder, util
from logging.handlers import TimedRotatingFileHandler
import logging

STRATEGY_NAME  = '__STRATEGY_NAME__'
ACCOUNT_ID     = '__ACCOUNT_ID__'
PORT           = __PORT__
CLIENT_ID      = __CLIENT_ID__
EMAIL          = '__EMAIL__'
CENTRAL_API_URL = '__CENTRAL_API_URL__'
SYMBOL         = '__SYMBOL__'
SEC_TYPE       = '__SEC_TYPE__'
EXCHANGE       = '__EXCHANGE__'
CURRENCY       = '__CURRENCY__'
LOT_SIZE       = __LOT_SIZE__
MAX_CAPITAL    = __MAX_CAPITAL__
BAR_SIZE       = '__BAR_SIZE__'
INTERVAL_MINS  = __INTERVAL_MINUTES__
STOP_LOSS_PCT  = __STOP_LOSS_PCT__
TAKE_PROFIT_PCT= __TAKE_PROFIT_PCT__
HAS_SHORT      = __HAS_SHORT__
KILL_SWITCH_PCT= __KILL_SWITCH_PCT__
MAX_ORDERS_DAY = 50
ORDER_TIMEOUT  = 30

EST = pytz.timezone('US/Eastern')
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

logger = logging.getLogger(STRATEGY_NAME)
logger.setLevel(logging.INFO)
_fh = TimedRotatingFileHandler(os.path.join(LOG_DIR, f'{STRATEGY_NAME}.log'),
                                when='midnight', backupCount=7, encoding='utf-8')
_fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
_ch = logging.StreamHandler()
_ch.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(_fh)
logger.addHandler(_ch)

ib = IB()
contract = None
current_position = 0
entry_price = 0.0
entry_time = ''
data_buffer = deque(maxlen=500)
risk_state = {}

def build_contract():
    if SEC_TYPE == 'STK':
        if EXCHANGE == 'SEHK': return Stock(SYMBOL, 'SEHK', 'HKD')
        elif EXCHANGE == 'SGX': return Stock(SYMBOL, 'SGX', 'SGD')
        else: return Stock(SYMBOL, 'SMART', 'USD')
    elif SEC_TYPE == 'CASH':
        return Forex(pair=SYMBOL, currency=CURRENCY)
    elif SEC_TYPE == 'FUT':
        return ContFuture(SYMBOL, exchange=EXCHANGE, currency=CURRENCY)
    else:
        return Stock(SYMBOL, EXCHANGE, CURRENCY)

def ensure_csv():
    if not os.path.exists(TRADES_FILE):
        with open(TRADES_FILE, 'w', newline='') as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

def log_trade(side, qty, price, commission, exec_id, pnl=0.0, signal=''):
    ensure_csv()
    with open(TRADES_FILE, 'a', newline='') as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow({
            'execId': exec_id, 'datetime': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy': STRATEGY_NAME, 'symbol': SYMBOL, 'secType': SEC_TYPE,
            'exchange': EXCHANGE, 'currency': CURRENCY, 'side': side,
            'quantity': qty, 'price': round(price, 6), 'commission': round(commission, 4),
            'pnl': round(pnl, 2), 'orderRef': STRATEGY_NAME, 'bar_size': BAR_SIZE, 'signal': signal})
    logger.info(f'[TRADE] {side} {qty} {SYMBOL} @ {price:.6f} | pnl=${pnl:+.2f} | signal={signal}')
    _trade_payload = {'email': EMAIL, 'strategy_id': STRATEGY_NAME, 'symbol': SYMBOL,
                      'side': 'buy' if side=='BOT' else 'sell', 'price': float(price),
                      'qty': float(qty), 'pnl': float(pnl)}
    # Report to local app (so Trades tab updates live)
    try: requests.post('http://127.0.0.1:8080/api/trade', json=_trade_payload, timeout=2)
    except Exception: pass
    # Report to central (instructor dashboard) — optional, fire-and-forget
    if CENTRAL_API_URL:
        try: requests.post(f'{CENTRAL_API_URL}/api/trade', json=_trade_payload, timeout=3)
        except Exception: pass

def save_state():
    try:
        json.dump({'current_position': current_position, 'entry_price': entry_price,
                   'entry_time': entry_time}, open(POSITION_FILE, 'w'))
        json.dump(risk_state, open(RISK_FILE, 'w'))
    except Exception as e: logger.warning(f'State save: {e}')

def load_state():
    global current_position, entry_price, entry_time, risk_state
    try:
        if os.path.exists(POSITION_FILE):
            s = json.load(open(POSITION_FILE))
            current_position = int(s.get('current_position', 0))
            entry_price = float(s.get('entry_price', 0.0))
            entry_time = str(s.get('entry_time', ''))
        risk_state = {'daily_loss': 0.0, 'session_pnl': 0.0, 'order_count': 0, 'halted': False}
        if os.path.exists(RISK_FILE):
            saved = json.load(open(RISK_FILE))
            today = datetime.now(EST).strftime('%Y-%m-%d')
            if saved.get('date', '') != today:
                saved.update({'daily_loss': 0.0, 'order_count': 0, 'date': today, 'halted': False})
            risk_state.update(saved)
    except Exception as e: logger.warning(f'State load: {e}')

def check_risk():
    if risk_state.get('halted'): return True, 'Halted by kill switch'
    dl = risk_state.get('daily_loss', 0.0)
    kill = MAX_CAPITAL * KILL_SWITCH_PCT
    if dl <= -kill:
        risk_state['halted'] = True
        return True, f'Kill switch: loss=${dl:.2f}'
    if risk_state.get('order_count', 0) >= MAX_ORDERS_DAY:
        return True, f'Max orders ({MAX_ORDERS_DAY})'
    return False, ''

async def place_order(action, signal=''):
    global current_position, entry_price, entry_time
    halt, reason = check_risk()
    if halt:
        logger.warning(f'Order blocked: {reason}')
        return 0.0, 0.0, ''
    order = MarketOrder(action, LOT_SIZE)
    order.orderRef = STRATEGY_NAME
    order.tif = 'DAY'
    logger.info(f'>>> {action} {LOT_SIZE} {SYMBOL} @ MKT | signal={signal}')
    trade = ib.placeOrder(contract, order)
    loop = asyncio.get_event_loop()
    deadline = loop.time() + ORDER_TIMEOUT
    while loop.time() < deadline:
        await asyncio.sleep(0.3)
        if trade.orderStatus.status == 'Filled': break
        if trade.orderStatus.status in ('Cancelled', 'Inactive', 'ApiCancelled'):
            logger.error(f'Order {trade.orderStatus.status}')
            return 0.0, 0.0, ''
    if trade.orderStatus.status != 'Filled':
        logger.error(f'Fill timeout {ORDER_TIMEOUT}s')
        return 0.0, 0.0, ''
    fp = float(trade.orderStatus.avgFillPrice or 0.0)
    fq = int(trade.orderStatus.filled or LOT_SIZE)
    await asyncio.sleep(1.5)
    comm = sum(float(f.commissionReport.commission) for f in trade.fills
               if f.commissionReport and f.commissionReport.commission and f.commissionReport.commission > 0)
    eid = trade.fills[0].execution.execId if trade.fills else f'{STRATEGY_NAME}_{datetime.utcnow():%Y%m%d%H%M%S%f}'
    pnl = 0.0
    if action == 'SELL' and current_position == 1 and entry_price > 0:
        pnl = (fp - entry_price) * fq
    elif action == 'BUY' and current_position == -1 and entry_price > 0:
        pnl = (entry_price - fp) * fq
    if action == 'BUY':
        current_position, entry_price, entry_time = 1, fp, datetime.utcnow().isoformat()
    else:
        current_position, entry_price, entry_time = 0, 0.0, ''
    risk_state['daily_loss'] = risk_state.get('daily_loss', 0.0) + pnl
    risk_state['session_pnl'] = risk_state.get('session_pnl', 0.0) + pnl
    risk_state['order_count'] = risk_state.get('order_count', 0) + 1
    risk_state['date'] = datetime.now(EST).strftime('%Y-%m-%d')
    log_trade('BOT' if action == 'BUY' else 'SLD', fq, fp, comm, eid, pnl, signal)
    save_state()
    return fp, comm, eid

# ── Signal helpers ──────────────────────────────────────────────────────────
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

# ── Signal computation (GENERATED) ─────────────────────────────────────────
__SIGNAL_CODE__

# ── Data fetch ──────────────────────────────────────────────────────────────
async def fetch_bars(duration=None):
    dur_map = {1:'2 D',5:'5 D',15:'10 D',60:'30 D',1440:'1 Y'}
    if duration is None: duration = dur_map.get(INTERVAL_MINS, '5 D')
    bars = await ib.reqHistoricalDataAsync(contract, endDateTime='', durationStr=duration,
        barSizeSetting=BAR_SIZE,
        whatToShow='MIDPOINT' if SEC_TYPE=='CASH' else 'TRADES',
        useRTH=True if SEC_TYPE=='STK' else False, formatDate=1)
    return [{'date':str(b.date),'open':float(b.open),'high':float(b.high),
             'low':float(b.low),'close':float(b.close),
             'volume':float(b.volume) if b.volume!=-1 else 0.0} for b in bars]

# ── Bar loop ────────────────────────────────────────────────────────────────
async def bar_loop():
    global current_position, data_buffer
    logger.info(f'Bar loop: polling every {INTERVAL_MINS}m')
    last_date = list(data_buffer)[-1].get('date','') if data_buffer else ''
    while True:
        try:
            await asyncio.sleep(INTERVAL_MINS * 60)
            halt, reason = check_risk()
            if halt: logger.warning(f'Halted: {reason}'); break
            bars = await fetch_bars()
            if not bars: continue
            new = [b for b in bars if b['date'] > last_date]
            if not new: continue
            data_buffer.extend(new)
            last_date = new[-1]['date']
            logger.info(f'Bar: {new[-1]["date"]} close={new[-1]["close"]:.5f}')
            sigs = compute_signals(list(data_buffer))
            if not sigs or sigs[-1] is None: continue
            sig, price = sigs[-1], list(data_buffer)[-1]['close']
            if sig == 'buy' and current_position == 0:
                await place_order('BUY', 'entry_long')
            elif sig == 'sell' and current_position == 1:
                await place_order('SELL', 'exit_long')
            elif sig == 'short' and current_position == 0 and HAS_SHORT:
                await place_order('SELL', 'entry_short')
            elif sig == 'cover' and current_position == -1 and HAS_SHORT:
                await place_order('BUY', 'exit_short')
            # SL/TP
            if current_position != 0 and entry_price > 0:
                if current_position == 1:
                    if STOP_LOSS_PCT > 0 and price <= entry_price*(1-STOP_LOSS_PCT):
                        await place_order('SELL', 'stop_loss')
                    elif TAKE_PROFIT_PCT > 0 and price >= entry_price*(1+TAKE_PROFIT_PCT):
                        await place_order('SELL', 'take_profit')
                elif current_position == -1:
                    if STOP_LOSS_PCT > 0 and price >= entry_price*(1+STOP_LOSS_PCT):
                        await place_order('BUY', 'stop_loss')
                    elif TAKE_PROFIT_PCT > 0 and price <= entry_price*(1-TAKE_PROFIT_PCT):
                        await place_order('BUY', 'take_profit')
        except Exception as e:
            logger.exception(f'Bar loop error: {e}')
            await asyncio.sleep(10)

async def heartbeat_loop():
    while True:
        try:
            await asyncio.sleep(300)
            d = {1:'LONG',-1:'SHORT',0:'FLAT'}.get(current_position,'?')
            pnl = 0.0
            if data_buffer and current_position != 0 and entry_price > 0:
                pnl = (list(data_buffer)[-1]['close'] - entry_price) * current_position * LOT_SIZE
            logger.info(f'[HB] pos={d} entry={entry_price:.5f} unrealised=${pnl:+.2f} session=${risk_state.get("session_pnl",0):.2f}')
        except asyncio.CancelledError: break
        except Exception: pass

async def connect_retry():
    for a in range(1, 6):
        try:
            await ib.connectAsync('127.0.0.1', PORT, CLIENT_ID, timeout=15)
            logger.info(f'Connected [attempt {a}]')
            return True
        except Exception as e:
            w = 2**a; logger.warning(f'Attempt {a} failed: {e}. Retry {w}s...'); await asyncio.sleep(w)
    return False

async def main():
    global contract, data_buffer
    logger.info('='*72)
    logger.info(f'{STRATEGY_NAME} | {SYMBOL} {SEC_TYPE} {EXCHANGE} | Port {PORT} | Lot {LOT_SIZE}')
    logger.info(f'SL {STOP_LOSS_PCT*100:.1f}% TP {TAKE_PROFIT_PCT*100:.1f}% | Bar {BAR_SIZE} | Short={HAS_SHORT}')
    logger.info('='*72)
    load_state()
    if not await connect_retry():
        logger.error('IBKR connect failed after 5 attempts'); return
    contract = build_contract()
    try:
        q = await ib.qualifyContractsAsync(contract)
        if q: contract = q[0]; logger.info(f'Contract: {contract.symbol} conId={contract.conId}')
        else: logger.error('Qualification failed'); ib.disconnect(); return
    except Exception as e: logger.error(f'Contract error: {e}'); ib.disconnect(); return
    logger.info('Fetching warmup bars...')
    warmup = await fetch_bars()
    if not warmup: logger.error('No warmup bars'); ib.disconnect(); return
    data_buffer = deque(warmup, maxlen=500)
    logger.info(f'Warmup: {len(warmup)} bars | last={warmup[-1]["date"]} close={warmup[-1]["close"]:.5f}')
    ensure_csv()
    if current_position != 0:
        logger.warning(f'[RESTORED] {"LONG" if current_position==1 else "SHORT"} @ {entry_price:.5f}')
    try:
        await asyncio.gather(bar_loop(), heartbeat_loop())
    except Exception as e: logger.exception(f'Error: {e}')
    finally:
        save_state()
        if ib.isConnected(): ib.disconnect()
        logger.info('Shutdown complete.')

if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Ctrl+C'); save_state()
        if ib.isConnected(): ib.disconnect()
'''
