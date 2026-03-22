import json
import logging
from lbank.websocket.base_lbank_socket import LbankSocketHandler


class LbankWebsocketClient:

    def __init__(
        self,
        base_url="",
        on_message=None,
        on_open=None,
        on_close=None,
        on_error=None,
        on_ping=None,
        on_pong=None,
        logger=None,
        timeout=None,
    ):
        if not logger:
            logger = logging.getLogger(__name__)
        self.logger = logger
        self.socket_manager = self._initialize_socket(
            base_url,
            on_message,
            on_open,
            on_close,
            on_error,
            on_ping,
            on_pong,
            logger,
            timeout,
        )

        self.socket_manager.start()

    def _initialize_socket(
        self,
        base_url,
        on_message,
        on_open,
        on_close,
        on_error,
        on_ping,
        on_pong,
        logger,
        timeout,
    ):
        return LbankSocketHandler(
            base_url,
            on_message=on_message,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_ping=on_ping,
            on_pong=on_pong,
            logger=logger,
            timeout=timeout,
        )

    def send(self, message: dict):
        self.socket_manager.send_message(json.dumps(message))

    def ping(self):
        self.socket_manager.ping()

    def stop(self):
        self.socket_manager.close()
        self.socket_manager.join()
