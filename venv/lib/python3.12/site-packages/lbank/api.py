from json import JSONDecodeError

import logging
import aiohttp

import hmac
import hashlib
from base64 import b64encode
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5

from lbank import __version__
from lbank.error import ServerError, CommonError
from lbank.utils import (
    RSA_STR,
    HMACSHA256_STR,
    ALLOW_METHOD,
    API_HMACSHA,
    get_timestamp,
    random_str,
    build_md5,
)


class HttpClient:

    def __init__(
        self,
        sign_method: str = "RSA",
        api_key: str = None,
        api_secret: str = None,
        private_key: str = None,
        base_url: str = "",
    ):
        sign_method = sign_method.upper()
        if sign_method not in ALLOW_METHOD:
            raise CommonError(f"{sign_method} sign method is not supported!")
        if sign_method == HMACSHA256_STR:
            sign_method = API_HMACSHA
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.sign_method = sign_method
        self.private_key = private_key
        self.random_str = None
        self.timestamp = None
        self.session = aiohttp.ClientSession()
        # self.headers = {
        #     "Content-Type": "application/x-www-form-urlencoded",
        #     "User-Agent": "lbank-connector-python/" + __version__,
        #     "signature_method": self.sign_method,
        # }
        self._logger = logging.getLogger(__name__)

    async def get(self, url, **kwargs):
        async with self.session.get(url, **kwargs) as response:
            return await response.text()

    async def post(self, url, **kwargs):
        async with self.session.post(url, **kwargs) as response:
            return await response.text()

    async def put(self, url, **kwargs):
        async with self.session.put(url, **kwargs) as response:
            return await response.text()

    async def delete(self, url, **kwargs):
        async with self.session.delete(url, **kwargs) as response:
            return await response.text()

    async def request(self, method, path, headers=None, **kwargs):
        url = self.base_url + path
        try:
            async with getattr(self.session, method.lower())(
                url, headers=headers, **kwargs
            ) as response:
                if response.status != 200:
                    raise ServerError(response.status, response.reason)
                try:
                    data = await response.json()
                except JSONDecodeError:
                    raise CommonError("response is not json format")
                # code = data.get("code", None)
                # if code is None or code != 200:
                #     raise ClientError(
                #         code,
                #         data.get("error_code", ""),
                #         data.get("error_message", ""),
                #     )
                return data
        except aiohttp.ClientError as e:
            raise CommonError(str(e))

    def build_header(self) -> dict:
        """
        build request header
        @return:dict:
        """
        self.random_str = random_str()
        self.timestamp = get_timestamp()
        return {
            "Content-Type": "application/json",
            "signature_method": self.sign_method,
            "User-Agent": "lbank-connector-python/" + __version__.__version__,
            "timestamp": self.timestamp,
            "echostr": self.random_str,
        }

    def build_rsasignv2(self, payload: dict) -> str:
        """
        build the sign
        """
        if self.private_key is None:
            raise Exception("private key is empty!")

        msg = build_md5(payload)
        private_key = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            + self.private_key
            + "\n-----END RSA PRIVATE KEY-----"
        )
        pri_key = PKCS1_v1_5.new(RSA.importKey(private_key))
        digest = SHA256.new(msg.encode("utf8"))
        sign = b64encode(pri_key.sign(digest))
        return sign.decode("utf8")

    def build_hmacsha256(self, payload: dict) -> str:
        """
        build the signature of the HmacSHA256
        """
        msg = build_md5(payload)
        api_secret = bytes(self.api_secret, encoding="utf8")
        payload = bytes(msg, encoding="utf8")
        signature = (
            hmac.new(
                api_secret, payload, digestmod=hashlib.sha256
            ).hexdigest().lower()
        )
        return signature

    def build_payload(self, payload: dict) -> dict:
        """
        @param payload: request form
        @return:
        """
        payload["api_key"] = self.api_key
        payload["timestamp"] = self.timestamp
        payload["signature_method"] = self.sign_method
        payload["echostr"] = self.random_str

        if self.sign_method == RSA_STR:
            payload["sign"] = self.build_rsasignv2(payload)
        elif self.sign_method == API_HMACSHA:
            payload["sign"] = self.build_hmacsha256(payload)
        else:
            raise CommonError(
                f"{self.sign_method} sign method is not supported!")
        return payload

    async def http_request(self, method, path, payload: dict = None, **kwargs):
        """
        :param method:
        :param path:
        :param payload:
        :param kwargs:
        :return:
        """

        if payload is None:
            payload = {}

        headers = self.build_header()

        payload = self.build_payload(payload)

        if "signature_method" in payload:
            del payload["signature_method"]
        if "echostr" in payload:
            del payload["echostr"]
        if "timestamp" in payload:
            del payload["timestamp"]
        if method.upper() == "GET":
            res = await self.request(
                method=method, path=path, headers=headers, params=payload
            )
        else:
            res = await self.request(
                method=method, path=path, headers=headers, json=payload
            )
        return res

    async def close(self):
        await self.session.close()
