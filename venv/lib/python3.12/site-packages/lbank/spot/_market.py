class Market(object):
    """Represents a market in the LBank API.

    Attributes:
        None

    Methods:
        system_ping: Sends a ping request to the LBank API.
        market_depth: Retrieves the market depth for a specific symbol.
        market_trades: Retrieves the recent trades for a specific symbol.
        market_ticker_price: Retrieves the ticker price for a specific symbol.
        market_book_ticker: Retrieves the book ticker for a specific symbol.
    """

    async def system_ping(self):
        path = "/v2/supplement/system_ping.do"
        return self.http_request("GET", path)

    async def market_depth(self, symbol, limit="100"):
        path = "/v2/supplement/incrDepth.do"
        payload = {"symbol": symbol, "limit": limit}
        return self.http_request("POST", path, payload=payload)

    async def market_trades(self, symbol, size="", time=""):
        path = "/v2/supplement/trades.do"
        payload = {"symbol": symbol, "size": size, "time": time}
        return self.http_request("POST", path, payload=payload)

    async def market_ticker_price(self, symbol=""):
        path = "/v2/supplement/ticker/price.do"
        payload = {"symbol": symbol}
        return self.http_request("GET", path, payload=payload)

    async def market_book_ticker(self, symbol):
        path = "/v2/supplement/ticker/bookTicker.do"
        payload = {"symbol": symbol}
        return self.http_request("GET", path, payload=payload)
