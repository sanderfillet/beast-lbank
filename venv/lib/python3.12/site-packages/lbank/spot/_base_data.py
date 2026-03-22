class BaseData(object):
    """Class representing the base data for the LBank API."""

    async def get_pairs(self):
        """Get the available currency pairs."""
        path = "/v2/currencyPairs.do"
        return self.http_request("GET", path)

    async def get_cny(self):
        """Get the USD to CNY exchange rate."""
        path = "/v2/usdToCny.do"
        return self.http_request("GET", path)

    async def get_code_configs(self, code):
        """Get the withdrawal configurations for a specific asset code.

        Args:
            code (str): The asset code.

        Returns:
            dict: The withdrawal configurations.
        """
        path = "/v2/withdrawConfigs.do"
        payload = {"assetCode": code}
        return self.http_request("GET", path, payload=payload)

    async def get_accuracy(self):
        """Get the accuracy settings for trading pairs."""
        path = "/v2/accuracy.do"
        return self.http_request("GET", path)

    async def get_timestamp(self):
        """Get the server timestamp."""
        path = "/v2/timestamp.do"
        return self.http_request("GET", path)

    async def get_etf_ticker(self):
        """Get the ETF ticker information."""
        path = "/v2/etfTicker/24hr.do"
        return self.http_request("GET", path)

    async def get_ticker(self, symbol):
        """Get the ticker information for a specific symbol.

        Args:
            symbol (str): The trading symbol.

        Returns:
            dict: The ticker information.
        """
        path = "/v2/ticker.do"
        payload = {"symbol": symbol}
        return self.http_request("GET", path, payload=payload)

    async def get_depth(self, symbol, size=60):
        """Get the depth information for a specific symbol.

        Args:
            symbol (str): The trading symbol.
            size (int, optional): The number of depth levels to retrieve. Defaults to 60.

        Returns:
            dict: The depth information.
        """
        path = "/v2/depth.do"
        payload = {"symbol": symbol, "size": size}
        return self.http_request("GET", path, payload=payload)

    async def get_incr_depth(self, symbol):
        """Get the incremental depth information for a specific symbol.

        Args:
            symbol (str): The trading symbol.

        Returns:
            dict: The incremental depth information.
        """
        path = "/v2/incrementalDepth.do"
        payload = {"symbol": symbol}
        return self.http_request("GET", path, payload=payload)

    async def get_trades(self, symbol, size=60, time=""):
        """Get the trade history for a specific symbol.

        Args:
            symbol (str): The trading symbol.
            size (int, optional): The number of trades to retrieve. Defaults to 60.
            time (str, optional): The time range for the trades. Defaults to empty string.

        Returns:
            dict: The trade history.
        """
        path = "/v2/trades.do"
        payload = {"symbol": symbol, "size": size, "time": time}
        return self.http_request("GET", path, payload=payload)

    async def get_kline(self, symbol, time, size=60, type="minute1"):
        """Get the kline data for a specific symbol.

        Args:
            symbol (str): The trading symbol.
            time (str): The time range for the kline data.
            size (int, optional): The number of kline data points to retrieve. Defaults to 60.
            type (str, optional): The type of kline data. Defaults to "minute1".

        Returns:
            dict: The kline data.
        """
        path = "/v2/kline.do"
        payload = {"symbol": symbol, "type": type, "size": size, "time": time}
        return self.http_request("GET", path, payload=payload)