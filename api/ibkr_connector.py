"""QuantX — IBKR connector wrapping ib_insync."""

try:
    from ib_insync import IB, Stock, MarketOrder, LimitOrder
    HAS_IB = True
except ImportError:
    HAS_IB = False

import logging

log = logging.getLogger("quantx-ibkr")


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
            self.ib.connect(self.host, self.port, clientId=self.client_id)
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
        try:
            contract = self._make_contract(symbol, exchange, currency)
            self.ib.qualifyContracts(contract)
            ticker = self.ib.reqMktData(contract)
            self.ib.sleep(2)
            price = ticker.last or ticker.bid or ticker.close
            log.info(f"Quote {symbol}: {price}")
            return price
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
