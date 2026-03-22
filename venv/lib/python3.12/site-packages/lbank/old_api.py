import requests
from json import JSONDecodeError
import json

import logging

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


class BlockHttpClient:

    def __init__(
        self,
        sign_method: str = "RSA",
        api_key: str = None,
        api_secret: str = None,
        base_url: str = "",
        log_level: int = logging.WARNING,
        is_json: bool = True,
    ):
        sign_method = sign_method.upper()
        if sign_method not in ALLOW_METHOD:
            raise CommonError(f"{sign_method} sign method is not supported!")
        if sign_method == HMACSHA256_STR:
            sign_method = API_HMACSHA
        self.is_json = is_json
        if is_json:
            self.content_type = "application/json"
        else:
            self.content_type = "application/x-www-form-urlencoded"
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.sign_method = sign_method
        self.random_str = None
        self.timestamp = None
        self.session = requests.Session()
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(log_level)

        handler = logging.StreamHandler()
        handler.setLevel(log_level)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    def get(self, url, **kwargs):
        response = self.session.get(url, **kwargs)
        return response.json()

    def post(self, url, **kwargs):
        response = self.session.post(url, **kwargs)
        return response.json()

    def put(self, url, **kwargs):
        response = self.session.put(url, **kwargs)
        return response.json()

    def delete(self, url, **kwargs):
        response = self.session.delete(url, **kwargs)
        return response.json()

    def request(self, method, path, headers=None, **kwargs):
        url = self.base_url + path
        try:
            response = getattr(self.session, method.lower())(
                url, headers=headers, **kwargs
            )
            if response.status_code != 200:
                raise ServerError(response.status_code, response.reason)
            try:
                data = response.json()
            except JSONDecodeError:
                raise CommonError(
                    f"response is not json format response is {response.text}"
                )
            # code = data.get("error_code", 0)
            # if code is None or code != 200:
            #     msg = data.get("msg", "")
            #     raise ClientError(
            #         response.status_code,
            #         code,
            #         msg
            #     )
            return data
        except requests.RequestException as e:
            raise CommonError(str(e))

    def build_header(self) -> dict:
        """
        build request header
        @return:dict:
        """
        self.random_str = random_str()
        self.timestamp = get_timestamp()
        return {
            "Content-Type": self.content_type,
            "signature_method": self.sign_method,
            "User-Agent": "lbank-connector-python/" + __version__.__version__,
            "timestamp": self.timestamp,
            "echostr": self.random_str,
        }

    def build_rsasignv2(self, payload: dict) -> str:
        """
        build the sign
        """
        if self.api_secret is None:
            raise Exception("private key is empty!")

        msg = build_md5(payload)
        private_key = (
            "-----BEGIN RSA PRIVATE KEY-----\n"
            + self.api_secret
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
                api_secret,
                payload, digestmod=hashlib.sha256).hexdigest().lower()
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

    def http_request(self, method, path, payload: dict = None, **kwargs):
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
        self._logger.debug(f"request path is {path}, payload is {payload}")
        if method.upper() == "GET":
            res = self.request(
                method=method, path=path, headers=headers, params=payload)
        else:
            if self.is_json:
                json_data = json.dumps(payload)
                res = self.request(
                    method=method, path=path, headers=headers, data=json_data
                )
            else:
                res = self.request(
                    method=method, path=path, headers=headers, data=payload
                )

        return res

    def close(self):
        self.session.close()
