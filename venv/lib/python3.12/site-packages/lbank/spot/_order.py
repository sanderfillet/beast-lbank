class Order(object):
    """
    Represents a collection of methods for interacting with orders on the LBank exchange.
    """

    async def create_order(
            self, symbol, type, price, amount, custom_id="", window=0):
        """
        Creates a new order on the LBank exchange.

        Args:
            symbol (str): The trading symbol.
            type (str): The order type.
            price (float): The order price.
            amount (float): The order amount.
            custom_id (str, optional): The custom order ID. Defaults to "".
            window (int, optional): The time window for the order. Defaults to 0.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/create_order.do"
        payload = {
            "symbol": symbol, "type": type, "price": price, "amount": amount,
            "custom_id": custom_id, "window": window}
        return self.http_request("POST", path, payload=payload)

    async def batch_create_order(self, orders: list, window=0):
        """
        Creates multiple orders in a single API call.

        Args:
            orders (list): A list of order objects.
            window (int, optional): The time window for the orders. Defaults to 0.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/batch_create_order.do"
        payload = {"orders": orders, "window": window}
        return self.http_request("POST", path, payload=payload)

    async def cancel_order(self, symbol, order_id):
        """
        Cancels an existing order on the LBank exchange.

        Args:
            symbol (str): The trading symbol.
            order_id (str): The order ID.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/cancel_order.do"
        payload = {"symbol": symbol, "order_id": order_id}
        return self.http_request("POST", path, payload=payload)

    async def cancel_client_orders(self, symbol, customer_id):
        """
        Cancels all orders placed by a specific customer on the LBank exchange.

        Args:
            symbol (str): The trading symbol.
            customer_id (str): The customer ID.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/cancel_clientOrders.do"
        payload = {"symbol": symbol, "customer_id": customer_id}
        return self.http_request("POST", path, payload=payload)

    async def get_order_info(self, symbol, order_id):
        """
        Retrieves information about a specific order on the LBank exchange.

        Args:
            symbol (str): The trading symbol.
            order_id (str): The order ID.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/orders_info.do"
        payload = {"symbol": symbol, "order_id": order_id}
        return self.http_request("POST", path, payload=payload)

    async def get_order_history(
            self, symbol, current_page, page_length, status):
        """
        Retrieves the order history for a specific trading symbol on the LBank exchange.

        Args:
            symbol (str): The trading symbol.
            current_page (int): The current page number.
            page_length (int): The number of orders per page.
            status (str): The order status.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/orders_info_history.do"
        payload = {
            "symbol": symbol,
            "current_page": current_page,
            "page_length": page_length,
            "status": status
        }
        return self.http_request("POST", path, payload=payload)

    async def get_transaction_detail(
        self, symbol, order_id
    ):
        """
        Retrieves the transaction detail for a specific order on the LBank exchange.

        Args:
            symbol (str): The trading symbol.
            order_id (str): The order ID.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/order_transaction_detail.do"
        payload = {"symbol": symbol, "order_id": order_id}
        return self.http_request("POST", path, payload=payload)

    async def get_transaction_history(
            self, symbol, type, start_date="", end_date="",
            f="", direct="", size=""):
        """
        Retrieves the transaction history for a specific trading symbol on the LBank exchange.

        Args:
            symbol (str): The trading symbol.
            type (str): The transaction type.
            start_date (str, optional): The start date. Defaults to "".
            end_date (str, optional): The end date. Defaults to "".
            f (str, optional): The from parameter. Defaults to "".
            direct (str, optional): The direction parameter. Defaults to "".
            size (str, optional): The size parameter. Defaults to "".

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/transaction_history.do"
        payload = {
            "symbol": symbol, "type": type, "start_date": start_date,
            "end_date": end_date, "from": f, "direct": direct, "size": size}
        return self.http_request("POST", path, payload=payload)

    async def query_open_order(
            self, symbol, current_page, page_length):
        """
        Retrieves the open order information for a specific trading symbol on the LBank exchange.

        Args:
            symbol (str): The trading symbol.
            current_page (int): The current page number.
            page_length (int): The number of orders per page.

        Returns:
            dict: The response from the API.

        Raises:
            Exception: If the API request fails.
        """
        path = "/v2/orders_info_no_deal.do"
        payload = {
            "symbol": symbol, "current_page": current_page,
            "page_length": page_length}
        return self.http_request("POST", path, payload=payload)
