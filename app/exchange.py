"""LBank Futures exchange client.

Uses the official LBank Contract API:
    Base URL  : https://{domain}/
    Public    : /cfd/openApi/v1/pub/...
    Private   : /cfd/openApi/v1/prv/...

Authentication (HmacSHA256 method):
    1. Collect all request params + api_key, signature_method, timestamp, echostr.
    2. Sort params alphabetically by key.
    3. Build query string: key=val&key=val...
    4. MD5-hash the string → uppercase hex.
    5. HmacSHA256-sign the MD5 result with api_secret → hex digest.
    6. Send headers: Content-Type, timestamp, signature_method, echostr.
    7. Append sign to the payload.

Position assumptions (confirmed by user):
    - Isolated margin  (isCrossMargin = 0)
    - Two-way position mode (positionType = 1)
      → posiDirection: 0 = Long, 1 = Short

Order side mapping (two-way mode):
    Open  long  → side=BUY,  offsetFlag=0, posiDirection=0
    Open  short → side=SELL, offsetFlag=0, posiDirection=1
    Close long  → side=SELL, offsetFlag=1, posiDirection=0
    Close short → side=BUY,  offsetFlag=1, posiDirection=1

orderPriceType codes:
    0  = Limit price
    4  = Market (ten price levels) — used for all market orders

Product group: "SwapU" (USDT-margined perpetuals)
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import string
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import requests

from app.logging_setup import get_logger
from app.models import TradeSide

if TYPE_CHECKING:
    pass

logger = get_logger("app.exchange")

# ─────────────────────────── constants ───────────────────────────────────────

_PRODUCT_GROUP = "SwapU"
_EXCHANGE_ID = "Exchange"

# Public endpoints (no auth required)
_EP_TIME = "/cfd/openApi/v1/pub/getTime"
_EP_MARKET = "/cfd/openApi/v1/pub/marketData"

# Private endpoints (auth required)
_EP_ACCOUNT = "/cfd/openApi/v1/prv/account"
_EP_POSITION = "/cfd/openApi/v1/prv/position"
_EP_PLACE_ORDER = "/cfd/openApi/v1/prv/placeOrder"
_EP_CANCEL_ORDER = "/cfd/openApi/v1/prv/cancelOrder"
_EP_OPEN_ORDERS = "/cfd/openApi/v1/prv/order"
_EP_PLACE_SL_TP = "/cfd/openApi/v1/prv/placeStopProfitAndLossOrder"
_EP_MEMBER_INFO = "/cfd/openApi/v1/prv/getMemberInfo"


# ─────────────────────────── data classes ────────────────────────────────────

@dataclass
class OrderResult:
    """Result of an order placement."""
    success: bool
    order_id: str | None = None
    avg_price: float | None = None
    fill_price: float | None = None
    filled_size: float | None = None
    error: str | None = None


@dataclass
class PositionInfo:
    """Current position information."""
    symbol: str
    side: str          # "long" or "short"
    size: float        # position quantity
    entry_price: float
    unrealized_pnl: float = 0.0
    leverage: int = 1
    position_id: str = ""
    trade_unit_id: str = ""


# ─────────────────────────── exceptions ──────────────────────────────────────

class LBankExchangeError(Exception):
    pass


# ─────────────────────────── client ──────────────────────────────────────────

class LBankClient:
    """LBank USDT-margined perpetual futures client.

    Implements the /cfd/openApi/v1/ REST API with HmacSHA256 authentication.

    Two-way position mode, isolated margin.

    posiDirection encoding (two-way mode):
        0 → Long
        1 → Short

    offsetFlag encoding:
        0 → Open position
        1 → Close position
        5 → Close all

    orderPriceType encoding:
        0 → Limit
        4 → Market (ten price levels)
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://api.lbkex.com",
        sign_method: str = "HmacSHA256",
        timeout: int = 10,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        # Strip trailing slash for clean URL joins
        self.base_url = base_url.rstrip("/")
        self.sign_method = sign_method
        self.timeout = timeout
        self.is_connected = False
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    # ─────────────────────── authentication ──────────────────────────────────

    def _echostr(self) -> str:
        """Generate a random alphanumeric string 30–40 chars (required by LBank)."""
        raw = uuid.uuid4().hex + uuid.uuid4().hex  # 64 chars
        return raw[:36]

    def _sign(self, params: dict[str, Any]) -> str:
        """Produce a HmacSHA256 signature for params dict.

        Steps (per LBank docs):
            1. Sort params alphabetically by key.
            2. Encode as key=val&key=val query string.
            3. MD5-hash → uppercase.
            4. HmacSHA256 the MD5 result with api_secret → hex.
        """
        sorted_pairs = "&".join(
            f"{k}={v}" for k, v in sorted(params.items(), key=lambda x: x[0])
        )
        md5_upper = hashlib.md5(sorted_pairs.encode()).hexdigest().upper()
        signature = hmac.new(
            self.api_secret.encode(),
            md5_upper.encode(),
            hashlib.sha256,
        ).hexdigest()
        return signature

    def _auth_headers_and_extras(self) -> tuple[dict[str, str], dict[str, str]]:
        """Return (request headers, extra params to merge into payload).

        The three auth params (timestamp, signature_method, echostr) must appear
        in BOTH the request headers AND the params used for signing.
        """
        ts = str(int(time.time() * 1000))
        echostr = self._echostr()
        headers = {
            "timestamp": ts,
            "signature_method": self.sign_method,
            "echostr": echostr,
        }
        extras = {
            "timestamp": ts,
            "signature_method": self.sign_method,
            "echostr": echostr,
            "api_key": self.api_key,
        }
        return headers, extras

    # ─────────────────────── HTTP helpers ────────────────────────────────────

    def _get(self, endpoint: str, params: dict[str, Any] | None = None, auth: bool = True) -> dict:
        """Execute an authenticated GET request.

        Auth params go into the query string for GET requests.
        """
        params = dict(params or {})
        headers = {}
        if auth:
            auth_headers, extras = self._auth_headers_and_extras()
            params.update(extras)
            params["sign"] = self._sign(params)
            headers = auth_headers

        url = self.base_url + endpoint
        try:
            resp = self._session.get(url, params=params, headers=headers, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("lbank_get_failed", endpoint=endpoint, error=str(e))
            raise LBankExchangeError(f"GET {endpoint} failed: {e}") from e

    def _post(self, endpoint: str, body: dict[str, Any] | None = None) -> dict:
        """Execute an authenticated POST request.

        Auth params are merged into the JSON body for POST requests.
        """
        body = dict(body or {})
        auth_headers, extras = self._auth_headers_and_extras()
        body.update(extras)
        body["sign"] = self._sign(body)

        url = self.base_url + endpoint
        try:
            resp = self._session.post(
                url, json=body, headers=auth_headers, timeout=self.timeout
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("lbank_post_failed", endpoint=endpoint, error=str(e))
            raise LBankExchangeError(f"POST {endpoint} failed: {e}") from e

    @staticmethod
    def _ok(res: dict) -> bool:
        """Check whether an API response indicates success."""
        return res.get("success") is True or res.get("error_code") == 0

    # ─────────────────────── connection ──────────────────────────────────────

    def connect(self) -> None:
        """Verify connectivity by calling the public /getTime endpoint."""
        try:
            res = self._get(_EP_TIME, auth=False)
            if not self._ok(res):
                raise LBankExchangeError(f"getTime returned error: {res}")
            self.is_connected = True
            logger.info(
                "exchange_connected",
                base_url=self.base_url,
                sign_method=self.sign_method,
            )
        except Exception as e:
            logger.error("exchange_connect_failed", error=str(e))
            raise LBankExchangeError(f"Failed to connect to LBank: {e}") from e

    # ─────────────────────── symbol helpers ──────────────────────────────────

    def _contract_symbol(self, symbol: str) -> str:
        """Convert bare coin to LBank contract symbol.

        e.g. 'BTC' → 'BTCUSDT'
        """
        sym = symbol.upper()
        if not sym.endswith("USDT"):
            sym = sym + "USDT"
        return sym

    # ─────────────────────── market data ─────────────────────────────────────

    def get_mark_price(self, symbol: str) -> float | None:
        """Get the latest mark price for a symbol.

        Endpoint: GET /cfd/openApi/v1/pub/marketData
        Response list item fields: symbol, lastPrice, markedPrice, ...
        """
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._get(
                _EP_MARKET,
                params={"productGroup": _PRODUCT_GROUP},
                auth=False,
            )
            data = res if isinstance(res, list) else res.get("data", [])
            for item in data:
                if item.get("symbol") == contract_symbol:
                    # Prefer markedPrice; fall back to lastPrice
                    price_str = item.get("markedPrice") or item.get("lastPrice")
                    if price_str:
                        return float(price_str)
            logger.warning("get_mark_price_not_found", symbol=contract_symbol)
            return None
        except Exception as e:
            logger.error("get_mark_price_failed", symbol=symbol, error=str(e))
            return None

    def get_all_prices(self) -> dict[str, float]:
        """Get mark prices for all listed symbols.

        Returns: {bare_symbol: price} e.g. {"BTC": 65000.0, "ETH": 3200.0}

        Endpoint: GET /cfd/openApi/v1/pub/marketData
        """
        try:
            res = self._get(
                _EP_MARKET,
                params={"productGroup": _PRODUCT_GROUP},
                auth=False,
            )
            data = res if isinstance(res, list) else res.get("data", [])
            prices: dict[str, float] = {}
            for item in data:
                raw_symbol = item.get("symbol", "")  # e.g. "BTCUSDT"
                price_str = item.get("markedPrice") or item.get("lastPrice")
                if raw_symbol and price_str:
                    # Strip trailing USDT for internal key consistency
                    bare = raw_symbol.replace("USDT", "") if raw_symbol.endswith("USDT") else raw_symbol
                    prices[bare] = float(price_str)
            return prices
        except Exception as e:
            logger.error("get_all_prices_failed", error=str(e))
            return {}

    # ─────────────────────── account ─────────────────────────────────────────

    def get_account_value(self) -> float | None:
        """Get wallet balance (USDT) for the futures account.

        Endpoint: GET /cfd/openApi/v1/prv/account
        Required params: asset=USDT, productGroup=SwapU
        Response fields: balance, available, unrealizedProfit, ...
        """
        try:
            res = self._get(
                _EP_ACCOUNT,
                params={"asset": "USDT", "productGroup": _PRODUCT_GROUP},
            )
            if self._ok(res) and "data" in res:
                data = res["data"]
                # balance = wallet balance (includes unrealised PnL)
                balance_str = data.get("balance") or data.get("available")
                if balance_str:
                    return float(balance_str)
            logger.warning("get_account_value_empty", res=res)
            return None
        except Exception as e:
            logger.error("get_account_value_failed", error=str(e))
            return None

    # ─────────────────────── positions ───────────────────────────────────────

    def get_position(self, symbol: str) -> PositionInfo | None:
        """Get current open position for a symbol (two-way mode).

        Endpoint: GET /cfd/openApi/v1/prv/position
        Required: productGroup=SwapU
        Optional: symbol=BTCUSDT

        Response fields (per position):
            posiDirection  : "0"=Long, "1"=Short, "2"=Net
            position       : quantity held
            openPrice      : average open price
            unrealizedProfit
            positionID
            tradeUnitID    : needed for closing in two-way mode
        """
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._get(
                _EP_POSITION,
                params={"productGroup": _PRODUCT_GROUP, "symbol": contract_symbol},
            )
            # Response is a list directly or wrapped in data
            positions = res if isinstance(res, list) else res.get("data", [])
            if not isinstance(positions, list):
                positions = []

            for pos in positions:
                size_str = pos.get("position", "0")
                size = float(size_str) if size_str else 0.0
                if size == 0:
                    continue

                posi_dir = str(pos.get("posiDirection", ""))
                if posi_dir == "0":
                    side = "long"
                elif posi_dir == "1":
                    side = "short"
                else:
                    continue  # net/unknown — skip

                return PositionInfo(
                    symbol=symbol,
                    side=side,
                    size=size,
                    entry_price=float(pos.get("openPrice", 0)),
                    unrealized_pnl=float(pos.get("unrealizedProfit", 0)),
                    leverage=int(float(pos.get("leverage", 1))),
                    position_id=str(pos.get("positionID", "")),
                    trade_unit_id=str(pos.get("tradeUnitID", "")),
                )
            return None
        except Exception as e:
            logger.error("get_position_failed", symbol=symbol, error=str(e))
            return None

    def get_all_positions(self) -> list[PositionInfo]:
        """Get all open positions across all symbols."""
        try:
            res = self._get(
                _EP_POSITION,
                params={"productGroup": _PRODUCT_GROUP},
            )
            positions_raw = res if isinstance(res, list) else res.get("data", [])
            if not isinstance(positions_raw, list):
                return []

            result: list[PositionInfo] = []
            for pos in positions_raw:
                size_str = pos.get("position", "0")
                size = float(size_str) if size_str else 0.0
                if size == 0:
                    continue
                posi_dir = str(pos.get("posiDirection", ""))
                if posi_dir == "0":
                    side = "long"
                elif posi_dir == "1":
                    side = "short"
                else:
                    continue
                raw_symbol = str(pos.get("symbol", ""))
                bare = raw_symbol.replace("USDT", "") if raw_symbol.endswith("USDT") else raw_symbol
                result.append(PositionInfo(
                    symbol=bare,
                    side=side,
                    size=size,
                    entry_price=float(pos.get("openPrice", 0)),
                    unrealized_pnl=float(pos.get("unrealizedProfit", 0)),
                    leverage=int(float(pos.get("leverage", 1))),
                    position_id=str(pos.get("positionID", "")),
                    trade_unit_id=str(pos.get("tradeUnitID", "")),
                ))
            return result
        except Exception as e:
            logger.error("get_all_positions_failed", error=str(e))
            return []

    # ─────────────────────── order helpers ───────────────────────────────────

    def _posi_direction(self, side: TradeSide) -> str:
        """Map TradeSide to LBank posiDirection string (two-way mode).

        Long  → "0"
        Short → "1"
        """
        return "0" if side == TradeSide.LONG else "1"

    def _open_side(self, side: TradeSide) -> str:
        """Buy/sell direction for opening a position.

        Open long  → BUY
        Open short → SELL
        """
        return "BUY" if side == TradeSide.LONG else "SELL"

    def _close_side(self, side: TradeSide) -> str:
        """Buy/sell direction for closing a position.

        Close long  → SELL
        Close short → BUY
        """
        return "SELL" if side == TradeSide.LONG else "BUY"

    def calculate_order_size(self, symbol: str, size_usd: float) -> float | None:
        """Calculate order quantity (contracts) from a USD notional size."""
        try:
            price = self.get_mark_price(symbol)
            if not price:
                return None
            return round(size_usd / price, 4)
        except Exception as e:
            logger.error("calculate_order_size_failed", symbol=symbol, error=str(e))
            return None

    # ─────────────────────── place market order ───────────────────────────────

    def place_market_order(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
    ) -> OrderResult:
        """Open a position at market price.

        Endpoint: POST /cfd/openApi/v1/prv/placeOrder
        offsetFlag = 0  (open position)
        orderPriceType = 4  (market — ten price levels)
        origType = 0  (regular order)

        Two-way mode fields:
            posiDirection: "0"=Long, "1"=Short
            side: "BUY" for long open, "SELL" for short open
        """
        try:
            contract_symbol = self._contract_symbol(symbol)
            body = {
                "symbol": contract_symbol,
                "side": self._open_side(side),
                "offsetFlag": "0",          # open position
                "orderPriceType": "4",      # market (ten price levels)
                "origType": "0",            # regular
                "posiDirection": self._posi_direction(side),
                "volume": str(size),
                "resultType": "ACK",        # returns full order info
            }

            res = self._post(_EP_PLACE_ORDER, body)

            if self._ok(res):
                data = res.get("data") or {}
                order_id = str(data.get("orderId") or data.get("orderSysID") or "")
                avg_price_str = data.get("avgPrice")
                avg_price = float(avg_price_str) if avg_price_str else None
                filled_qty_str = data.get("executedQty")
                filled_qty = float(filled_qty_str) if filled_qty_str else size

                logger.info(
                    "order_success",
                    operation="market_open",
                    order_id=order_id,
                    symbol=symbol,
                    side=side.value,
                    size=size,
                )
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    avg_price=avg_price,
                    fill_price=avg_price,
                    filled_size=filled_qty,
                )
            else:
                error = res.get("msg") or str(res)
                logger.error("order_failed", symbol=symbol, side=side.value, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error("market_order_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    # ─────────────────────── close position ──────────────────────────────────

    def close_position(
        self,
        symbol: str,
        side: TradeSide | None = None,
        size: float | None = None,
    ) -> OrderResult:
        """Close an existing position at market price.

        If side is None, attempts to close all (offsetFlag=5).
        If size is provided, closes that quantity. Otherwise closes all.

        Endpoint: POST /cfd/openApi/v1/prv/placeOrder
        offsetFlag = 1 (close position) or 5 (close all)
        """
        try:
            price = self.get_mark_price(symbol)
            if not price:
                return OrderResult(success=False, error="Could not get mark price")

            contract_symbol = self._contract_symbol(symbol)

            if side is None:
                # Close all without caring about direction
                body = {
                    "symbol": contract_symbol,
                    "side": "SELL",         # placeholder; offsetFlag=5 closes regardless
                    "offsetFlag": "5",      # close all
                    "orderPriceType": "4",  # market
                    "origType": "0",
                    "resultType": "ACK",
                }
            else:
                offset = "1"  # close specific position
                if size is None:
                    offset = "5"  # close all of this direction

                body = {
                    "symbol": contract_symbol,
                    "side": self._close_side(side),
                    "offsetFlag": offset,
                    "orderPriceType": "4",
                    "origType": "0",
                    "posiDirection": self._posi_direction(side),
                    "resultType": "ACK",
                }
                if size is not None:
                    body["volume"] = str(size)

            res = self._post(_EP_PLACE_ORDER, body)

            if self._ok(res):
                data = res.get("data") or {}
                order_id = str(data.get("orderId") or data.get("orderSysID") or "")
                avg_price_str = data.get("avgPrice")
                avg_price = float(avg_price_str) if avg_price_str else price

                logger.info(
                    "order_success",
                    operation="close_position",
                    order_id=order_id,
                    symbol=symbol,
                    side=side.value if side else "all",
                )
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    fill_price=avg_price,
                    avg_price=avg_price,
                )
            else:
                error = res.get("msg") or str(res)
                logger.error("close_position_failed", symbol=symbol, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error("close_position_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    # ─────────────────────── stop loss / take profit ──────────────────────────

    def _place_sl_tp_order(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        sl_price: float | None,
        tp_price: float | None,
        operation: str,
    ) -> OrderResult:
        """Internal helper: place a combined or single SL/TP order.

        Endpoint: POST /cfd/openApi/v1/prv/placeStopProfitAndLossOrder

        Key fields:
            profitAndLossDirection : "0"=Long's SL/TP, "1"=Short's SL/TP
            direction              : "1"=Sell (close long), "0"=Buy (close short)
            offsetFlag             : "1"=close position
            triggerOrderType       : "2"=Order TP/SL
            triggerPriceType       : "0"=latest price
            triggerPriceCalType    : "0"=by absolute price
            price                  : 0 for market execution
        """
        try:
            contract_symbol = self._contract_symbol(symbol)
            is_long = (side == TradeSide.LONG)

            body: dict[str, Any] = {
                "exchangeID": _EXCHANGE_ID,
                "instrumentID": contract_symbol,
                "direction": "1" if is_long else "0",   # sell to close long, buy to close short
                "offsetFlag": "1",                       # close position
                "posiDirection": self._posi_direction(side),
                "profitAndLossDirection": "0" if is_long else "1",
                "triggerOrderType": "2",                 # order TP/SL
                "triggerPriceType": "0",                 # latest price
                "triggerPriceCalType": "0",              # by absolute price
                "price": "0",                            # market execution price
                "volume": str(size),
                "resultType": "ACK",
            }

            if sl_price is not None:
                body["closeSLTriggerPrice"] = str(sl_price)
                body["closeSLPrice"] = ""   # blank = market fill on trigger

            if tp_price is not None:
                body["closeTPTriggerPrice"] = str(tp_price)
                body["closeTPPrice"] = ""   # blank = market fill on trigger

            res = self._post(_EP_PLACE_SL_TP, body)

            if self._ok(res):
                data = res.get("data") or {}
                order_id = str(data.get("orderSysID") or data.get("orderId") or "")
                logger.info(
                    "order_success",
                    operation=operation,
                    order_id=order_id,
                    symbol=symbol,
                    sl_price=sl_price,
                    tp_price=tp_price,
                )
                return OrderResult(success=True, order_id=order_id)
            else:
                error = res.get("msg") or str(res)
                logger.error(f"{operation}_failed", symbol=symbol, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error(f"{operation}_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    def place_stop_loss(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        trigger_price: float,
    ) -> OrderResult:
        """Place a stop-loss order.

        Uses placeStopProfitAndLossOrder with only closeSLTriggerPrice set.
        Fills at market price on trigger.
        """
        return self._place_sl_tp_order(
            symbol, side, size,
            sl_price=trigger_price,
            tp_price=None,
            operation="stop_loss",
        )

    def place_take_profit(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        trigger_price: float,
    ) -> OrderResult:
        """Place a take-profit order.

        Uses placeStopProfitAndLossOrder with only closeTPTriggerPrice set.
        Fills at market price on trigger.
        """
        return self._place_sl_tp_order(
            symbol, side, size,
            sl_price=None,
            tp_price=trigger_price,
            operation="take_profit",
        )

    def modify_stop_loss(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        new_trigger_price: float,
        old_order_id: str | None = None,
    ) -> OrderResult:
        """Modify an existing stop-loss by cancelling and replacing.

        LBank does not have a direct modify endpoint for SL/TP orders,
        so we cancel the old one and place a new one.
        """
        try:
            if old_order_id:
                self.cancel_order(symbol, old_order_id, order_type="plan")

            return self.place_stop_loss(symbol, side, size, new_trigger_price)

        except Exception as e:
            logger.error("modify_stop_loss_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    # ─────────────────────── cancel orders ───────────────────────────────────

    def cancel_order(
        self,
        symbol: str,
        order_id: str,
        order_type: str = "price",
    ) -> bool:
        """Cancel a single order.

        Endpoint: POST /cfd/openApi/v1/prv/cancelOrder
        order_type: "price" for regular limit/market orders,
                    "plan" for trigger/SL/TP orders.

        If order_id is empty, cancels all orders for the symbol.
        """
        try:
            contract_symbol = self._contract_symbol(symbol)
            body: dict[str, Any] = {
                "symbol": contract_symbol,
                "orderType": order_type,
            }
            if order_id:
                body["orderId"] = order_id

            res = self._post(_EP_CANCEL_ORDER, body)
            ok = self._ok(res)
            if not ok:
                logger.warning(
                    "cancel_order_nok",
                    symbol=symbol,
                    order_id=order_id,
                    msg=res.get("msg"),
                )
            return ok
        except Exception as e:
            logger.error("cancel_order_failed", symbol=symbol, order_id=order_id, error=str(e))
            return False

    def cancel_all_orders(self, symbol: str) -> None:
        """Cancel all open regular and plan orders for a symbol."""
        # Cancel regular (limit/market) orders
        self.cancel_order(symbol, "", order_type="price")
        # Cancel plan (trigger / SL / TP) orders
        self.cancel_order(symbol, "", order_type="plan")

    # ─────────────────────── open orders ─────────────────────────────────────

    def get_open_orders(self, symbol: str) -> list[dict]:
        """Get all currently open (unfilled) orders for a symbol.

        Endpoint: GET /cfd/openApi/v1/prv/order
        Required: productGroup=SwapU
        Optional: orderType (default "price")

        Response fields per order:
            orderId, side, posiDirection, positionSide,
            price, origQty, executedQty, status, symbol,
            sltriggerPrice, tptriggerPrice, stopPrice, ...

        Status codes:
            0 = Not set
            1 = Fully filled
            2 = Partially filled, not cancelled
            3 = Partially filled, cancelled
            4 = Not filled, not cancelled  ← open
            6 = No fill, cancelled
        """
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._get(
                _EP_OPEN_ORDERS,
                params={
                    "productGroup": _PRODUCT_GROUP,
                    "symbol": contract_symbol,
                    "pageNo": "1",
                    "pageSize": "100",
                },
            )
            if self._ok(res):
                data = res.get("data", {})
                result_list = data.get("resultList", [])
                # Filter to only unfilled orders (status == "4")
                return [o for o in result_list if str(o.get("status", "")) == "4"]
            return []
        except Exception as e:
            logger.error("get_open_orders_failed", symbol=symbol, error=str(e))
            return []
