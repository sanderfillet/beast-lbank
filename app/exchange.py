from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import ccxt
from lbank.old_api import BlockHttpClient

from app.logging_setup import get_logger
from app.models import TradeSide

if TYPE_CHECKING:
    pass

logger = get_logger("app.exchange")


@dataclass
class OrderResult:
    """Result of an order placement."""
    success: bool
    order_id: str | None = None
    fill_price: float | None = None
    filled_size: float | None = None
    error: str | None = None


@dataclass
class PositionInfo:
    """Current position information."""
    symbol: str
    side: str
    size: float
    entry_price: float
    unrealized_pnl: float = 0.0
    leverage: int = 1


class LBankExchangeError(Exception):
    pass


class LBankClient:
    """LBank Contract API client.

    Handles all order placement and position management
    using the official LBank Python connector.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://api.lbkex.com/",
        sign_method: str = "HmacSHA256",
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.sign_method = sign_method
        self.is_connected = False
        self._client: BlockHttpClient | None = None
        self._ccxt: ccxt.Exchange | None = None

    def connect(self) -> None:
        """Initialize LBank client and verify connection."""
        try:
            self._client = BlockHttpClient(
                sign_method=self.sign_method,
                api_key=self.api_key,
                api_secret=self.api_secret,
                base_url=self.base_url,
                log_level=logging.WARNING,
            )

            # CCXT for candle data and prices
            self._ccxt = ccxt.binance({"enableRateLimit": True})

            # Test connection
            res = self._client.http_request("get", "v2/accuracy.do")
            self.is_connected = True
            logger.info(
                "exchange_connected",
                base_url=self.base_url,
                sign_method=self.sign_method,
            )
        except Exception as e:
            logger.error("exchange_connect_failed", error=str(e))
            raise LBankExchangeError(f"Failed to connect to LBank: {e}") from e

    def _contract_symbol(self, symbol: str) -> str:
        """Convert symbol to LBank contract format.
        e.g. 'BTC' -> 'btc_usdt'
        """
        return f"{symbol.lower()}_usdt"

    def get_mark_price(self, symbol: str) -> float | None:
        """Get current mark price for a symbol."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._client.http_request(
                "get",
                "v2/ticker.do",
                payload={"symbol": contract_symbol},
            )
            if res and "data" in res:
                return float(res["data"]["ticker"]["latest"])
            return None
        except Exception as e:
            logger.error("get_mark_price_failed", symbol=symbol, error=str(e))
            return None

    def get_all_prices(self) -> dict[str, float]:
        """Get mark prices for all symbols."""
        try:
            res = self._client.http_request("get", "v2/ticker.do", payload={"symbol": "all"})
            prices = {}
            if res and "data" in res:
                for item in res["data"]:
                    symbol = item["symbol"].split("_")[0].upper()
                    prices[symbol] = float(item["ticker"]["latest"])
            return prices
        except Exception as e:
            logger.error("get_all_prices_failed", error=str(e))
            return {}

    def get_position(self, symbol: str) -> PositionInfo | None:
        """Get current position for a symbol."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._client.http_request(
                "post",
                "v2/supplement/user_info_account.do",
                payload={},
            )
            if not res or "data" not in res:
                return None

            # Parse positions from account info
            for pos in res.get("data", {}).get("openPositions", []):
                if pos.get("symbol") == contract_symbol:
                    size = float(pos.get("amount", 0))
                    if size == 0:
                        return None
                    return PositionInfo(
                        symbol=symbol,
                        side="long" if pos.get("type") == "open_long" else "short",
                        size=size,
                        entry_price=float(pos.get("openPrice", 0)),
                        unrealized_pnl=float(pos.get("profit", 0)),
                    )
            return None
        except Exception as e:
            logger.error("get_position_failed", symbol=symbol, error=str(e))
            return None

    def place_market_order(
        self,
        symbol: str,
        side: TradeSide,
        size_usd: float,
        slippage: float = 0.02,
    ) -> OrderResult:
        """Place a market order on LBank futures."""
        try:
            price = self.get_mark_price(symbol)
            if not price:
                return OrderResult(success=False, error="Could not get mark price")

            # Calculate size in contracts
            size = round(size_usd / price, 4)
            contract_symbol = self._contract_symbol(symbol)

            # LBank order type: open_long, open_short
            order_type = "open_long" if side == TradeSide.LONG else "open_short"

            payload = {
                "symbol": contract_symbol,
                "type": order_type,
                "price": str(price),
                "amount": str(size),
                "match_best_price": "1",  # Market order
            }

            res = self._client.http_request("post", "v2/supplement/create_order.do", payload=payload)

            if res and res.get("result") == "true":
                order_id = res.get("data", {}).get("orderId", "")
                logger.info(
                    "order_success",
                    operation="market_open",
                    order_id=order_id,
                    symbol=symbol,
                    side=side.value,
                    size=size,
                    price=price,
                )
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    fill_price=price,
                    filled_size=size,
                )
            else:
                error = str(res)
                logger.error("order_failed", symbol=symbol, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error("market_order_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    def place_stop_loss(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        trigger_price: float,
    ) -> OrderResult:
        """Place a stop loss order."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            # SL for long = close_long, SL for short = close_short
            order_type = "close_long" if side == TradeSide.LONG else "close_short"

            payload = {
                "symbol": contract_symbol,
                "type": order_type,
                "price": str(trigger_price),
                "amount": str(size),
                "trigger_price": str(trigger_price),
                "order_type": "stop",
            }

            res = self._client.http_request(
                "post",
                "v2/supplement/create_plan_order.do",
                payload=payload,
            )

            if res and res.get("result") == "true":
                order_id = res.get("data", {}).get("orderId", "")
                logger.info(
                    "order_success",
                    operation="stop_loss",
                    order_id=order_id,
                    symbol=symbol,
                    trigger_price=trigger_price,
                )
                return OrderResult(success=True, order_id=order_id)
            else:
                error = str(res)
                logger.error("stop_loss_failed", symbol=symbol, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error("stop_loss_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    def place_take_profit(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        trigger_price: float,
    ) -> OrderResult:
        """Place a take profit order."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            order_type = "close_long" if side == TradeSide.LONG else "close_short"

            payload = {
                "symbol": contract_symbol,
                "type": order_type,
                "price": str(trigger_price),
                "amount": str(size),
                "trigger_price": str(trigger_price),
                "order_type": "take_profit",
            }

            res = self._client.http_request(
                "post",
                "v2/supplement/create_plan_order.do",
                payload=payload,
            )

            if res and res.get("result") == "true":
                order_id = res.get("data", {}).get("orderId", "")
                logger.info(
                    "order_success",
                    operation="take_profit",
                    order_id=order_id,
                    symbol=symbol,
                    trigger_price=trigger_price,
                )
                return OrderResult(success=True, order_id=order_id)
            else:
                error = str(res)
                logger.error("take_profit_failed", symbol=symbol, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error("take_profit_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    def close_position(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        slippage: float = 0.02,
    ) -> OrderResult:
        """Close an existing position at market price."""
        try:
            price = self.get_mark_price(symbol)
            if not price:
                return OrderResult(success=False, error="Could not get mark price")

            contract_symbol = self._contract_symbol(symbol)
            order_type = "close_long" if side == TradeSide.LONG else "close_short"

            payload = {
                "symbol": contract_symbol,
                "type": order_type,
                "price": str(price),
                "amount": str(size),
                "match_best_price": "1",
            }

            res = self._client.http_request(
                "post",
                "v2/supplement/create_order.do",
                payload=payload,
            )

            if res and res.get("result") == "true":
                order_id = res.get("data", {}).get("orderId", "")
                logger.info(
                    "order_success",
                    operation="close_position",
                    order_id=order_id,
                    symbol=symbol,
                    side=side.value,
                )
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    fill_price=price,
                )
            else:
                error = str(res)
                logger.error("close_position_failed", symbol=symbol, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error("close_position_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    def modify_stop_loss(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        new_trigger_price: float,
        old_order_id: str | None = None,
    ) -> OrderResult:
        """Modify an existing stop loss by cancelling and replacing."""
        try:
            # Cancel old SL
            if old_order_id:
                contract_symbol = self._contract_symbol(symbol)
                self._client.http_request(
                    "post",
                    "v2/supplement/cancel_plan_order.do",
                    payload={
                        "symbol": contract_symbol,
                        "orderId": old_order_id,
                    },
                )

            # Place new SL
            return self.place_stop_loss(symbol, side, size, new_trigger_price)

        except Exception as e:
            logger.error("modify_stop_loss_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel an open order."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._client.http_request(
                "post",
                "v2/supplement/cancel_plan_order.do",
                payload={
                    "symbol": contract_symbol,
                    "orderId": order_id,
                },
            )
            return res and res.get("result") == "true"
        except Exception as e:
            logger.error("cancel_order_failed", symbol=symbol, order_id=order_id, error=str(e))
            return False

    def get_open_orders(self, symbol: str) -> list[dict]:
        """Get all open orders for a symbol."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._client.http_request(
                "post",
                "v2/supplement/orders_info_no_deal.do",
                payload={
                    "symbol": contract_symbol,
                    "current_page": "1",
                    "page_length": "50",
                },
            )
            if res and res.get("result") == "true":
                return res.get("data", {}).get("list", [])
            return []
        except Exception as e:
            logger.error("get_open_orders_failed", symbol=symbol, error=str(e))
            return []

    def get_account_value(self) -> float | None:
        """Get total account equity in USDT."""
        try:
            res = self._client.http_request(
                "post",
                "v2/supplement/user_info_account.do",
                payload={},
            )
            if res and "data" in res:
                return float(res["data"].get("totalBalance", 0))
            return None
        except Exception as e:
            logger.error("get_account_value_failed", error=str(e))
            return None

    def calculate_order_size(self, symbol: str, size_usd: float) -> float | None:
        """Calculate order size in contracts from USD size."""
        try:
            price = self.get_mark_price(symbol)
            if not price:
                return None
            return round(size_usd / price, 4)
        except Exception as e:
            logger.error("calculate_order_size_failed", symbol=symbol, error=str(e))
            return None

    def cancel_all_orders(self, symbol: str) -> None:
        """Cancel all open orders for a symbol."""
        try:
            orders = self.get_open_orders(symbol)
            for order in orders:
                self.cancel_order(symbol, order.get("orderId", ""))
        except Exception as e:
            logger.error("cancel_all_orders_failed", symbol=symbol, error=str(e))
