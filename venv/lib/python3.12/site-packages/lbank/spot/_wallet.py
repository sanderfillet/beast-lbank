class Wallet(object):
    """
    Represents a wallet for interacting with the LBank API.

    Methods:
    - get_system_status: Get the system status.
    - get_withdraw_user_info: Get the user's withdrawal information.
    - handle_withdraw: Handle a withdrawal request.
    - get_deposit_history: Get the deposit history.
    - get_supplement_withdraw_history: Get the withdrawal history.
    - get_supplement_deposit_address: Get the deposit address.
    - get_supplement_asset_detail: Get the asset details.
    - get_supplement_trade_fee: Get the trade fee.
    - get_supplement_auth: Get the API restrictions.
    """

    async def get_system_status(self, status="0"):
        path = "/v2/supplement/system_status.do"
        payload = {"status": status}
        return self.http_request("POST", path, payload=payload)

    async def get_withdraw_user_info(self):
        path = "/v2/supplement/user_info.do"
        return self.http_request("POST", path)

    async def handle_withdraw(
            self, address, networkName, coin, amount, fee, 
            memo="", mark="", name="", withdrawOrderId="", type=""):
        path = "/v2/supplement/withdraw.do"
        payload = {
            "address": address, "networkName": networkName, "coin": coin,
            "amount": amount, "fee": fee, "memo": memo, "mark": mark,
            "name": name, "withdrawOrderId": withdrawOrderId, "type": type
        }
        return self.http_request("POST", path, payload=payload)

    async def get_deposit_history(
            self, status="", coin="", startTime="", endTime=""):
        path = "/v2/supplement/deposit_history.do"
        payload = {
            "status": status, "coin": coin,
            "startTime": startTime, "endTime": endTime
        }
        return self.http_request("POST", path, payload=payload)

    async def get_supplement_withdraw_history(
            self, status="", coin="", withdrawOrderId="",
            startTime="", endTime=""):
        path = "/v2/supplement/withdraws.do"
        payload = {
            "status": status, "coin": coin, "withdrawOrderId": withdrawOrderId,
            "startTime": startTime, "endTime": endTime
        }
        return self.http_request("POST", path, payload=payload)

    async def get_supplement_deposit_address(
            self, coin, networkName=""):
        path = "/v2/supplement/get_deposit_address.do"
        payload = {"coin": coin, "networkName": networkName}
        return self.http_request("POST", path, payload=payload)

    async def get_supplement_asset_detail(self, coin=""):
        path = "/v2/supplement/asset_detail.do"
        payload = {"coin": coin}
        return self.http_request("POST", path, payload=payload)

    async def get_supplement_trade_fee(self, category=""):
        path = "/v2/supplement/customer_trade_fee.do"
        payload = {"category": category}
        return self.http_request("POST", path, payload=payload)

    async def get_supplement_auth(self):
        path = "/v2/supplement/api_Restrictions.do"
        return self.http_request("POST", path)
