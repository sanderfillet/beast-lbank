class Trade(object):
    """
    This class represents a trade in the LBank spot trading platform.

    Methods:
    - trade_test_create_order: Create a test order for trading.
    - trade_create_order: Create an order for trading.
    - trade_cancel_order: Cancel an order.
    - trade_cancel_all_order: Cancel all orders for a specific symbol.
    - trade_query_order: Query the details of an order.
    - trade_no_deal: Query the details of orders that have not been dealt.
    - trade_query_all_order: Query the details of all orders.
    - trade_account: Query the account information.
    - trade_transaction_history: Query the transaction history.

    """

    async def trade_test_create_order(
            self, symbol, type, price, amount, custom_id="", window=0):
        """
        Create a test order for trading.

        Args:
        - symbol (str): The trading symbol.
        - type (str): The order type.
        - price (float): The order price.
        - amount (float): The order amount.
        - custom_id (str, optional): The custom ID for the order. Defaults to "".
        - window (int, optional): The time window for the order. Defaults to 0.

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/create_order_test.do"
        payload = {
            "symbol": symbol, "type": type, "price": price, "amount": amount,
            "custom_id": custom_id, "window": window
        }
        return self.http_request("POST", path, payload=payload)

    async def trade_create_order(
            self, symbol, type, price, amount, custom_id=""):
        """
        Create an order for trading.

        Args:
        - symbol (str): The trading symbol.
        - type (str): The order type.
        - price (float): The order price.
        - amount (float): The order amount.
        - custom_id (str, optional): The custom ID for the order. Defaults to "".

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/create_order.do"
        payload = {
            "symbol": symbol, "type": type, "price": price, "amount": amount,
            "custom_id": custom_id
        }
        return self.http_request("POST", path, payload=payload)

    async def trade_cancel_order(
            self, symbol, orderId="", origClientOrderId=""):
        """
        Cancel an order.

        Args:
        - symbol (str): The trading symbol.
        - orderId (str, optional): The order ID. Defaults to "".
        - origClientOrderId (str, optional): The original client order ID. Defaults to "".

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/cancel_order.do"
        payload = {
            "symbol": symbol, "orderId": orderId, 
            "origClientOrderId": origClientOrderId
        }
        return self.http_request("POST", path, payload=payload)

    async def trade_cancel_all_order(self, symbol):
        """
        Cancel all orders for a specific symbol.

        Args:
        - symbol (str): The trading symbol.

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/cancel_order_by_symbol.do"
        payload = {"symbol": symbol}
        return self.http_request("POST", path, payload=payload)

    async def trade_query_order(
            self, symbol, orderId="", origClientOrderId=""):
        """
        Query the details of an order.

        Args:
        - symbol (str): The trading symbol.
        - orderId (str, optional): The order ID. Defaults to "".
        - origClientOrderId (str, optional): The original client order ID. Defaults to "".

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/orders_info.do"
        payload = {
            "symbol": symbol, "orderId": orderId,
            "origClientOrderId": origClientOrderId
        }
        return self.http_request("POST", path, payload=payload)

    async def trade_no_deal(self, symbol, current_page, page_length):
        """
        Query the details of orders that have not been dealt.

        Args:
        - symbol (str): The trading symbol.
        - current_page (int): The current page number.
        - page_length (int): The number of orders per page.

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/orders_info_no_deal.do"
        payload = {
            "symbol": symbol, "current_page": current_page,
            "page_length": page_length
        }
        return self.http_request("POST", path, payload=payload)

    async def trade_query_all_order(
            self, symbol, current_page, page_length, status=""):
        """
        Query the details of all orders.

        Args:
        - symbol (str): The trading symbol.
        - current_page (int): The current page number.
        - page_length (int): The number of orders per page.
        - status (str, optional): The order status. Defaults to "".

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/orders_info_history.do"
        payload = {
            "symbol": symbol, "current_page": current_page,
            "page_length": page_length, "status": status
        }
        return self.http_request("POST", path, payload=payload)

    async def trade_account(self):
        """
        Query the account information.

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/user_info_account.do"
        return self.http_request("POST", path)

    async def trade_transaction_history(
            self, symbol, startTim="", endTime="", fromId="", limit=""):
        """
        Query the transaction history.

        Args:
        - symbol (str): The trading symbol.
        - startTim (str, optional): The start time of the transaction history. Defaults to "".
        - endTime (str, optional): The end time of the transaction history. Defaults to "".
        - fromId (str, optional): The starting ID of the transaction history. Defaults to "".
        - limit (str, optional): The maximum number of transaction records to return. Defaults to "".

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/supplement/transaction_history.do"
        payload = {
            "symbol": symbol, "startTim": startTim,
            "endTime": endTime, "fromId": fromId, "limit": limit
        }
        return self.http_request("POST", path, payload=payload)
