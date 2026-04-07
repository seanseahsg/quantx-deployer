"""QuantX — Simple bot templates for direct order placement testing.

Uses __PLACEHOLDER__ style instead of {placeholder} to avoid conflicts
with literal curly braces in the Python code (dicts, json, f-strings).
"""

SIMPLE_LP_TEMPLATE = r'''#!/usr/bin/env python3
"""QuantX Simple LongPort Bot — places one limit buy, waits, cancels."""
import sys, os, math, decimal, time, logging, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EMAIL = '__EMAIL__'
SYMBOL = '__SYMBOL__'
CENTRAL_API_URL = '__CENTRAL_API_URL__'
LOG_DIR = '__LOG_DIR__'
LOG_NAME = '__LOG_NAME__'

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, LOG_NAME), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger('quantx-lp')

from api.database import get_db, decrypt
conn = get_db()
student = conn.execute("SELECT * FROM students WHERE email=?", (EMAIL,)).fetchone()
conn.close()

if not student:
    log.error("Student not found: %s", EMAIL)
    sys.exit(1)

app_key = decrypt(student['app_key_enc'])
app_secret = decrypt(student['app_secret_enc'])
access_token = decrypt(student['access_token_enc'])

log.info("=" * 50)
log.info("QuantX Simple LongPort Bot")
log.info("Email: %s | Symbol: %s", EMAIL, SYMBOL)
log.info("=" * 50)

from longport.openapi import Config, QuoteContext, TradeContext, OrderSide, OrderType, TimeInForceType

cfg = Config(app_key=app_key, app_secret=app_secret, access_token=access_token)
quote_ctx = QuoteContext(cfg)
trade_ctx = TradeContext(cfg)
log.info("LongPort connected")

quotes = quote_ctx.quote([SYMBOL])
price = float(quotes[0].last_done)
log.info("%s price: %s", SYMBOL, price)

def hk_tick_round(p):
    if p < 0.25: tick = 0.001
    elif p < 0.5: tick = 0.005
    elif p < 10: tick = 0.010
    elif p < 20: tick = 0.020
    elif p < 100: tick = 0.050
    elif p < 200: tick = 0.100
    elif p < 500: tick = 0.200
    elif p < 1000: tick = 0.500
    elif p < 2000: tick = 1.000
    elif p < 5000: tick = 2.000
    else: tick = 5.000
    return round(math.floor(p / tick) * tick, 4)

if SYMBOL.endswith('.HK'):
    limit_price = hk_tick_round(price * 0.98)
else:
    limit_price = round(price * 0.98, 2)

log.info("Placing limit BUY: 100 shares @ %s (2%% below market)", limit_price)

try:
    resp = trade_ctx.submit_order(
        symbol=SYMBOL,
        order_type=OrderType.LO,
        side=OrderSide.Buy,
        submitted_quantity=100,
        time_in_force=TimeInForceType.Day,
        submitted_price=decimal.Decimal(str(limit_price)),
        remark='QuantX bot order'
    )
    order_id = resp.order_id
    log.info("ORDER PLACED! ID: %s", order_id)

    if CENTRAL_API_URL:
        try:
            requests.post(CENTRAL_API_URL + '/api/trade', json={
                "email": EMAIL, "strategy_id": "SIMPLE_TEST",
                "symbol": SYMBOL, "side": "BUY",
                "price": float(limit_price), "qty": 100, "pnl": 0
            }, timeout=5)
        except Exception:
            pass

    log.info("Waiting 30 seconds before cancelling...")
    time.sleep(30)

    try:
        trade_ctx.cancel_order(order_id)
        log.info("Order cancelled: %s", order_id)
    except Exception as e:
        log.info("Cancel result: %s (may have auto-cancelled or filled)", e)

except Exception as e:
    log.error("Order failed: %s", e)

log.info("Bot finished.")
'''


SIMPLE_IBKR_TEMPLATE = r'''#!/usr/bin/env python3
"""QuantX Simple IBKR Bot — places one market buy, waits, cancels."""
import sys, os, time, logging, requests, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

asyncio.set_event_loop(asyncio.new_event_loop())

EMAIL = '__EMAIL__'
SYMBOL = '__SYMBOL__'
IBKR_HOST = '__IBKR_HOST__'
IBKR_PORT = __IBKR_PORT__
IBKR_CLIENT_ID = __IBKR_CLIENT_ID__
CENTRAL_API_URL = '__CENTRAL_API_URL__'
LOG_DIR = '__LOG_DIR__'
LOG_NAME = '__LOG_NAME__'

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, LOG_NAME), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger('quantx-ibkr')

log.info("=" * 50)
log.info("QuantX Simple IBKR Bot")
log.info("Email: %s | Symbol: %s", EMAIL, SYMBOL)
log.info("IBKR: %s:%s clientId=%s", IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID)
log.info("=" * 50)

from ib_insync import IB, Stock, MarketOrder
import math

ib = IB()
try:
    ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID, timeout=15)
    log.info("IBKR connected. Account: %s", ib.managedAccounts())
except Exception as e:
    log.error("IBKR connect failed: %s", e)
    sys.exit(1)

def make_contract(symbol):
    if symbol.endswith('.HK'):
        return Stock(symbol.replace('.HK', ''), 'SEHK', 'HKD')
    elif symbol.endswith('.SI'):
        return Stock(symbol.replace('.SI', ''), 'SGX', 'SGD')
    else:
        return Stock(symbol.replace('.US', ''), 'SMART', 'USD')

contract = make_contract(SYMBOL)
ib.qualifyContracts(contract)

ticker = ib.reqMktData(contract)
ib.sleep(3)
price = ticker.last if (ticker.last and not math.isnan(ticker.last)) else None
if not price:
    price = ticker.close if (ticker.close and not math.isnan(ticker.close)) else None
if not price:
    price = ticker.bid if (ticker.bid and not math.isnan(ticker.bid)) else None
log.info("%s live price: %s", SYMBOL, price)

if not price:
    bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='1 D',
        barSizeSetting='1 day', whatToShow='MIDPOINT', useRTH=True)
    if bars:
        price = bars[-1].close
    log.info("%s historical price: %s", SYMBOL, price)

if not price:
    log.error("Could not get price for %s - exiting", SYMBOL)
    ib.disconnect()
    sys.exit(1)

log.info("Placing market BUY: 1 share of %s", SYMBOL)
order = MarketOrder('BUY', 1)
try:
    trade = ib.placeOrder(contract, order)
    ib.sleep(2)
    log.info("ORDER PLACED! ID: %s Status: %s", trade.order.orderId, trade.orderStatus.status)

    if CENTRAL_API_URL:
        try:
            requests.post(CENTRAL_API_URL + '/api/trade', json={
                "email": EMAIL, "strategy_id": "IBKR_SIMPLE_TEST",
                "symbol": SYMBOL, "side": "BUY",
                "price": float(price), "qty": 1, "pnl": 0
            }, timeout=5)
        except Exception:
            pass

    log.info("Waiting 30 seconds before cancelling...")
    time.sleep(30)

    ib.cancelOrder(trade.order)
    ib.sleep(1)
    log.info("Order cancelled")

except Exception as e:
    log.error("Order failed: %s", e)

ib.disconnect()
log.info("Bot finished.")
'''
