class Account(object):
    """Represents a user account on the LBank exchange."""

    async def get_user_info(self):
        """Get the user's account information.

        Returns:
            dict: The user's account information.
        """
        path = "/v2/user_info.do"
        return self.http_request("POST", path)

    async def create_subscribe_key(self):
        """Create a new subscription key.

        Returns:
            dict: The newly created subscription key.
        """
        path = "/v2/subscribe/get_key.do"
        return self.http_request("POST", path)

    async def refresh_subscribe_key(self):
        """Refresh the subscription key.

        Returns:
            dict: The refreshed subscription key.
        """
        path = "/v2/subscribe/refresh_key.do"
        return self.http_request("POST", path)

    async def close_subscribe_key(self):
        """Close the subscription key.

        Returns:
            dict: The result of closing the subscription key.
        """
        path = "/v2/subscribe/destroy_key.do"
        return self.http_request("POST", path)

    async def get_deposit_address(self, assetCode="", netWork=""):
        """Get the deposit address for a specific asset.

        Args:
            assetCode (str, optional): The asset code. Defaults to "".
            netWork (str, optional): The network. Defaults to "".

        Returns:
            dict: The deposit address information.
        """
        path = "/v2/get_deposit_address.do"
        payload = {"assetCode": assetCode, "netWork": netWork}
        return self.http_request("POST", path, payload=payload)

    async def get_deposit_history(
            self, assetCode="", startTime="", endTime="",
            pageNo="1", pageSize="20"):
        """Get the deposit history.

        Args:
            assetCode (str, optional): The asset code. Defaults to "".
            startTime (str, optional): The start time. Defaults to "".
            endTime (str, optional): The end time. Defaults to "".
            pageNo (str, optional): The page number. Defaults to "1".
            pageSize (str, optional): The page size. Defaults to "20".

        Returns:
            dict: The deposit history.
        """
        path = "/v2/deposit_history.do"
        payload = {
            "assetCode": assetCode, "startTime": startTime, "endTime": endTime,
            "pageNo": pageNo, "pageSize": pageSize}
        return self.http_request("POST", path, payload=payload)
