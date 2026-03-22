import hashlib
import logging
import random
import string
import time


RSA_STR = "RSA"
HMACSHA256_STR = "HMACSHA256"
API_HMACSHA = "HmacSHA256"
ALLOW_METHOD = [
    RSA_STR,
    HMACSHA256_STR,
]


def get_timestamp() -> str:
    return str(int(time.time() * 1000)).split(".", maxsplit=1)[0]


def random_str() -> str:
    num = string.ascii_letters + string.digits
    return "".join(random.sample(num, 35))


def build_md5(payload: dict) -> str:
    """
    @param payload: request form
    @return:
    """
    params = [k + '=' + str(payload[k]) for k in sorted(payload.keys())]
    params = '&'.join(params)
    msg = hashlib.md5(params.encode("utf8")).hexdigest().upper()
    return msg


def init_logger(level, log_file: str = None):
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=level,
        filename=log_file,
        format="[%(asctime)s.%(msecs)03d] UTC %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
