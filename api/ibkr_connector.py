"""QuantX — IBKR connector wrapping ib_insync."""

try:
    from ib_insync import IB, Stock, MarketOrder, LimitOrder
    HAS_IB = True
except ImportError:
    HAS_IB = False

import logging
import hashlib

log = logging.getLogger("quantx-ibkr")


def get_client_id_for_strategy(strategy_id: str) -> int:
    """Generate a unique IBKR client ID (100-899) from strategy_id."""
    h = int(hashlib.md5(strategy_id.encode()).hexdigest(), 16)
    return (h % 800) + 100


class IBKRConnector:
    def __init__(self, host='127.0.0.1', port=7497, client_id=1):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB() if HAS_IB else None
        self._connected = False

    def connect(self):
        if not HAS_IB:
            log.error("ib_insync not installed. Run: pip install ib_insync")
            return False
        try:
            # Ensure event loop exists in this thread (needed if called from ThreadPoolExecutor)
            import asyncio
            try:
                asyncio.get_event_loop()
            except RuntimeError:
                asyncio.set_event_loop(asyncio.new_event_loop())
            self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=10)
            self._connected = self.ib.isConnected()
            log.info(f"IBKR connected: {self._connected}, account: {self.ib.managedAccounts()}")
            return self._connected
        except Exception as e:
            log.error(f"IBKR connect failed: {e}")
            return False

    def disconnect(self):
        if self.ib and self.ib.isConnected():
            self.ib.disconnect()
        self._connected = False

    def is_connected(self):
        return self.ib.isConnected() if self.ib else False

    def get_quote(self, symbol, exchange='SMART', currency='USD'):
        import math
        try:
            contract = self._make_contract(symbol, exchange, currency)
            self.ib.qualifyContracts(contract)
            # Try live market data first
            ticker = self.ib.reqMktData(contract)
            self.ib.sleep(2)
            # Check each price field, skip nan
            for field, name in [(ticker.last, 'last'), (ticker.bid, 'bid'),
                                (ticker.ask, 'ask'), (ticker.close, 'close')]:
                if field is not None and not (isinstance(field, float) and math.isnan(field)) and field > 0:
                    log.info(f"Quote {symbol}: {field} (from {name})")
                    return field
            # Fallback: request historical data (last close)
            log.info(f"Quote {symbol}: live data unavailable, trying historical fallback")
            try:
                bars = self.ib.reqHistoricalData(
                    contract, endDateTime='', durationStr='1 D',
                    barSizeSetting='1 day', whatToShow='MIDPOINT', useRTH=True
                )
                if bars:
                    price = bars[-1].close
                    log.info(f"Quote {symbol}: {price} (from historical fallback)")
                    return price
            except Exception as he:
                log.warning(f"Historical fallback failed for {symbol}: {he}")
            log.warning(f"Quote {symbol}: no price available from any source")
            return None
        except Exception as e:
            log.error(f"Quote failed {symbol}: {e}")
            return None

    def place_market_order(self, symbol, side, qty, exchange='SMART', currency='USD'):
        try:
            contract = self._make_contract(symbol, exchange, currency)
            self.ib.qualifyContracts(contract)
            order = MarketOrder(side, qty)
            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(1)
            log.info(f"Market order: {side} {qty} {symbol} orderId={trade.order.orderId}")
            return trade.order.orderId
        except Exception as e:
            log.error(f"Market order failed: {e}")
            return None

    def place_limit_order(self, symbol, side, qty, price, exchange='SMART', currency='USD'):
        try:
            contract = self._make_contract(symbol, exchange, currency)
            self.ib.qualifyContracts(contract)
            order = LimitOrder(side, qty, price)
            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(1)
            log.info(f"Limit order: {side} {qty} {symbol} @ {price} orderId={trade.order.orderId}")
            return trade.order.orderId
        except Exception as e:
            log.error(f"Limit order failed: {e}")
            return None

    def cancel_order(self, order_id):
        try:
            for t in self.ib.openTrades():
                if t.order.orderId == order_id:
                    self.ib.cancelOrder(t.order)
                    log.info(f"Cancelled order {order_id}")
                    return True
            return False
        except Exception as e:
            log.error(f"Cancel failed: {e}")
            return False

    def get_positions(self):
        try:
            return self.ib.positions()
        except Exception as e:
            log.error(f"Positions failed: {e}")
            return []

    def _make_contract(self, symbol, exchange='SMART', currency='USD'):
        if symbol.endswith('.HK'):
            return Stock(symbol.replace('.HK', ''), 'SEHK', 'HKD')
        elif symbol.endswith('.US'):
            return Stock(symbol.replace('.US', ''), 'SMART', 'USD')
        elif symbol.endswith('.SI'):
            return Stock(symbol.replace('.SI', ''), 'SGX', 'SGD')
        else:
            return Stock(symbol, exchange, currency)
