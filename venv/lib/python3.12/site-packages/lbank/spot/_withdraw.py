class Withdraw(object):
    """
    Withdraw class handles the withdrawal operations for the LBank API.

    Methods:
    - handle_withdraw: Handles the withdrawal of funds from the account.
    - canel_withdraw: Cancels a withdrawal request.
    - get_withdraw_history: Retrieves the withdrawal history for a specific asset.

    """

    async def handle_withdraw(
            self, account, assetCode, amount, memo="", mark="",
            fee="", type=""):
        """
        Handles the withdrawal of funds from the account.

        Args:
        - account (str): The account to withdraw from.
        - assetCode (str): The code of the asset to withdraw.
        - amount (float): The amount to withdraw.
        - memo (str, optional): Additional memo for the withdrawal. Default is an empty string.
        - mark (str, optional): Additional mark for the withdrawal. Default is an empty string.
        - fee (str, optional): The fee for the withdrawal. Default is an empty string.
        - type (str, optional): The type of withdrawal. Default is an empty string.

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/withdraw.do"
        payload = {
            "account": account, "assetCode": assetCode, "amount": amount,
            "memo": memo, "mark": mark, "fee": fee, "type": type}
        return self.http_request("POST", path, payload=payload)

    async def canel_withdraw(self, withdrawId):
        """
        Cancels a withdrawal request.

        Args:
        - withdrawId (str): The ID of the withdrawal request to cancel.

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/withdrawCancel.do"
        payload = {"withdrawId": withdrawId}
        return self.http_request("POST", path, payload=payload)

    async def get_withdraw_history(
            self, assetCode, status, pageNo="", pageSize=""):
        """
        Retrieves the withdrawal history for a specific asset.

        Args:
        - assetCode (str): The code of the asset to retrieve the history for.
        - status (str): The status of the withdrawals to retrieve.
        - pageNo (str, optional): The page number of the results. Default is an empty string.
        - pageSize (str, optional): The number of results per page. Default is an empty string.

        Returns:
        - dict: The response from the API.

        """
        path = "/v2/withdraws.do"
        payload = {
            "assetCode": assetCode, "status": status, "pageNo": pageNo,
            "pageSize": pageSize}
        return self.http_request("POST", path, payload=payload)
