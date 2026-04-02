"""LBank Futures exchange client.

Uses the official LBank Contract API:
    Base URL  : https://fapi.lbank.info
    Public    : /cfd/openApi/v1/pub/...
    Private   : /cfd/openApi/v1/prv/...

Authentication (RSA method):
    1. Collect all request params + api_key, signature_method, timestamp, echostr.
    2. Sort params alphabetically by key.
    3. Build query string: key=val&key=val...
    4. MD5-hash the string → uppercase hex.
    5. RSA-SHA256 sign the MD5 result with the private key → Base64.
    6. For GET: URL-encode the sign param (base64 +/= break query strings).
    7. For POST: append sign to the JSON body.

Position assumptions (confirmed by user):
    - Isolated margin  (isCrossMargin = 0)
    - Two-way position mode (positionType = 1)
      → posiDirection: 0 = Long, 1 = Short

Order flow (confirmed via live API testing 2026-04-01):
    Entry + SL + TP  → placeStopProfitAndLossOrder
                        direction=0 (long) / 1 (short)
                        offsetFlag=0 (open position with SL/TP attached)
    Update SL        → placeStopProfitAndLossPosition
                        direction=1 (long) / 0 (short)
                        offsetFlag=1 (close position)
    Close position   → placeOrder offsetFlag=1 (close) or 5 (close all)
    Cancel orders    → cancelOrder

Rate limits:
    Read-only : 50 requests per 10 seconds per API key
    Trading   : 1 request per 10 seconds per API key
    → All trading POST calls sleep _TRADE_DELAY after execution.
"""

from __future__ import annotations

import base64
import hashlib
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlencode

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from app.logging_setup import get_logger
from app.models import TradeSide

if TYPE_CHECKING:
    pass

logger = get_logger("app.exchange")

# ─────────────────────────── constants ───────────────────────────────────────

_PRODUCT_GROUP = "SwapU"
_EXCHANGE_ID   = "Exchange"
_TRADE_DELAY   = 1.0  # seconds between trading API calls (1 req/10s limit)

# Public endpoints
_EP_TIME   = "/cfd/openApi/v1/pub/getTime"
_EP_MARKET = "/cfd/openApi/v1/pub/marketData"

# Private endpoints
_EP_ACCOUNT        = "/cfd/openApi/v1/prv/account"
_EP_POSITION       = "/cfd/openApi/v1/prv/position"
_EP_PLACE_ORDER    = "/cfd/openApi/v1/prv/placeOrder"
_EP_CANCEL_ORDER   = "/cfd/openApi/v1/prv/cancelOrder"
_EP_OPEN_ORDERS    = "/cfd/openApi/v1/prv/order"
_EP_PLACE_SL_TP    = "/cfd/openApi/v1/prv/placeStopProfitAndLossOrder"
_EP_UPDATE_SL_TP   = "/cfd/openApi/v1/prv/placeStopProfitAndLossPosition"
_EP_INSTRUMENT     = "/cfd/openApi/v1/pub/instrument"


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
    side: str
    size: float
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

    Key design:
        - Entry orders always include SL + TP via placeStopProfitAndLossOrder
        - SL updates use placeStopProfitAndLossPosition
        - TP1 partial exits are handled by the monitor loop in software
        - All trading calls sleep _TRADE_DELAY to respect rate limits
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.lbank.info",
        sign_method: str = "RSA",
        timeout: int = 10,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.sign_method = sign_method
        self.timeout = timeout
        self.is_connected = False
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        # Cached instrument specs: {contract_symbol: {"volume_tick": float, "min_volume": float}}
        self._instrument_specs: dict[str, dict] = {}

    # ─────────────────────── auth ─────────────────────────────────────────────

    def _echostr(self) -> str:
        return (uuid.uuid4().hex + uuid.uuid4().hex)[:36]

    def _sign(self, params: dict) -> str:
        """RSA-SHA256 signature per LBank docs."""
        sorted_pairs = "&".join(
            f"{k}={v}" for k, v in sorted(params.items(), key=lambda x: x[0])
        )
        md5_upper = hashlib.md5(sorted_pairs.encode()).hexdigest().upper()
        private_key = serialization.load_der_private_key(
            base64.b64decode(self.api_secret), password=None,
        )
        signature = private_key.sign(
            md5_upper.encode(), padding.PKCS1v15(), hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _auth_headers_and_extras(self) -> tuple[dict, dict]:
        ts = str(int(time.time() * 1000))
        echo = self._echostr()
        headers = {"timestamp": ts, "signature_method": self.sign_method, "echostr": echo}
        extras  = {"timestamp": ts, "signature_method": self.sign_method, "echostr": echo, "api_key": self.api_key}
        return headers, extras

    # ─────────────────────── HTTP ─────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict[str, Any] | None = None, auth: bool = True) -> dict:
        """GET request. RSA sign is URL-encoded to protect base64 chars."""
        params = dict(params or {})
        headers = {}
        if auth:
            auth_headers, extras = self._auth_headers_and_extras()
            params.update(extras)
            sign = self._sign(params)
            query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
            query += f"&sign={quote(sign, safe='')}"
            headers = auth_headers
            url = self.base_url + endpoint + "?" + query
        else:
            url = self.base_url + endpoint
            if params:
                url += "?" + urlencode(params)
        try:
            resp = self._session.get(url, headers=headers, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("lbank_get_failed", endpoint=endpoint, error=str(e))
            raise LBankExchangeError(f"GET {endpoint} failed: {e}") from e

    def _post(self, endpoint: str, body: dict[str, Any] | None = None, is_trading: bool = True) -> dict:
        """POST request. is_trading=True sleeps _TRADE_DELAY after call."""
        body = dict(body or {})
        auth_headers, extras = self._auth_headers_and_extras()
        body.update(extras)
        body["sign"] = self._sign(body)
        url = self.base_url + endpoint
        try:
            resp = self._session.post(url, json=body, headers=auth_headers, timeout=self.timeout)
            resp.raise_for_status()
            result = resp.json()
            if is_trading:
                time.sleep(_TRADE_DELAY)
            return result
        except Exception as e:
            logger.error("lbank_post_failed", endpoint=endpoint, error=str(e))
            raise LBankExchangeError(f"POST {endpoint} failed: {e}") from e

    @staticmethod
    def _ok(res: dict) -> bool:
        return res.get("success") is True or res.get("error_code") == 0

    # ─────────────────────── connection ──────────────────────────────────────

    def connect(self) -> None:
        """Verify connectivity, then cache instrument specs for size rounding."""
        try:
            res = self._get(_EP_TIME, auth=False)
            if not self._ok(res):
                raise LBankExchangeError(f"getTime error: {res}")
            self.is_connected = True
            logger.info("exchange_connected", base_url=self.base_url, sign_method=self.sign_method)
        except Exception as e:
            logger.error("exchange_connect_failed", error=str(e))
            raise LBankExchangeError(f"Failed to connect: {e}") from e

        # Fetch and cache instrument specs for all SwapU symbols
        try:
            self._load_instrument_specs()
        except Exception as e:
            logger.warning("instrument_specs_load_failed", error=str(e))

    def _load_instrument_specs(self) -> None:
        """Fetch instrument specs and cache volumeTick + minOrderVolume per symbol.

        Endpoint: GET /cfd/openApi/v1/pub/instrument?productGroup=SwapU
        Key fields:
            symbol          : e.g. "HYPEUSDT"
            volumeTick      : minimum order size step (e.g. 0.1 for HYPE)
            minOrderVolume  : minimum order quantity (same as volumeTick in practice)
        """
        res = self._get(_EP_INSTRUMENT, params={"productGroup": _PRODUCT_GROUP}, auth=False)
        items = res if isinstance(res, list) else res.get("data", [])
        for item in items:
            symbol = item.get("symbol", "")
            if not symbol:
                continue
            volume_tick = float(item.get("volumeTick") or item.get("minOrderVolume") or 1)
            min_volume = float(item.get("minOrderVolume") or volume_tick)
            self._instrument_specs[symbol] = {
                "volume_tick": volume_tick,
                "min_volume": min_volume,
            }
        logger.info("instrument_specs_loaded", count=len(self._instrument_specs))

    # ─────────────────────── symbol ──────────────────────────────────────────

    def _contract_symbol(self, symbol: str) -> str:
        """'BTC' → 'BTCUSDT'"""
        sym = symbol.upper()
        return sym if sym.endswith("USDT") else sym + "USDT"

    # ─────────────────────── helpers ─────────────────────────────────────────

    def _posi_direction(self, side: TradeSide) -> str:
        """Long → '0', Short → '1'"""
        return "0" if side == TradeSide.LONG else "1"

    def _close_side(self, side: TradeSide) -> str:
        """Close long → SELL, Close short → BUY"""
        return "SELL" if side == TradeSide.LONG else "BUY"

    def calculate_order_size(self, symbol: str, size_usd: float) -> float | None:
        """Calculate order quantity from USD notional, rounded to volumeTick precision.

        Uses cached instrument specs to:
        1. Determine decimal precision from volumeTick (e.g. 0.1 → 1 decimal)
        2. Round DOWN to nearest volumeTick step (floor, not round)
        3. Enforce minOrderVolume floor

        Examples (HYPE at $36, volumeTick=0.1):
            $10 → 0.277... → floor to 0.2 HYPE
            $50 → 1.388... → floor to 1.3 HYPE
        """
        try:
            price = self.get_mark_price(symbol)
            if not price:
                return None

            contract_symbol = self._contract_symbol(symbol)
            specs = self._instrument_specs.get(contract_symbol, {})
            volume_tick = specs.get("volume_tick", 0.0001)
            min_volume = specs.get("min_volume", volume_tick)

            raw_size = size_usd / price

            # Floor to nearest volumeTick step (never send more than requested)
            import math
            size = math.floor(raw_size / volume_tick) * volume_tick

            # Determine decimal precision from volumeTick
            # e.g. 0.1 → 1dp, 0.01 → 2dp, 0.0001 → 4dp
            decimals = max(0, -int(math.floor(math.log10(volume_tick))))
            size = round(size, decimals)

            # Enforce minimum volume
            if size < min_volume:
                logger.warning(
                    "order_size_below_minimum",
                    symbol=symbol,
                    raw_size=round(raw_size, 8),
                    rounded_size=size,
                    min_volume=min_volume,
                    size_usd=size_usd,
                )
                return None

            logger.debug(
                "order_size_calculated",
                symbol=symbol,
                size_usd=size_usd,
                price=price,
                raw_size=round(raw_size, 8),
                volume_tick=volume_tick,
                final_size=size,
            )
            return size

        except Exception as e:
            logger.error("calculate_order_size_failed", symbol=symbol, error=str(e))
            return None

    # ─────────────────────── market data ─────────────────────────────────────

    def get_mark_price(self, symbol: str) -> float | None:
        """Get mark price for a symbol (public, no auth)."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._get(_EP_MARKET, params={"productGroup": _PRODUCT_GROUP}, auth=False)
            data = res if isinstance(res, list) else res.get("data", [])
            for item in data:
                if item.get("symbol") == contract_symbol:
                    price_str = item.get("markedPrice") or item.get("lastPrice")
                    if price_str:
                        return float(price_str)
            logger.warning("get_mark_price_not_found", symbol=contract_symbol)
            return None
        except Exception as e:
            logger.error("get_mark_price_failed", symbol=symbol, error=str(e))
            return None

    def get_all_prices(self) -> dict[str, float]:
        """Get mark prices for all symbols. Returns {bare_symbol: price}."""
        try:
            res = self._get(_EP_MARKET, params={"productGroup": _PRODUCT_GROUP}, auth=False)
            data = res if isinstance(res, list) else res.get("data", [])
            prices: dict[str, float] = {}
            for item in data:
                raw = item.get("symbol", "")
                price_str = item.get("markedPrice") or item.get("lastPrice")
                if raw and price_str:
                    bare = raw.replace("USDT", "") if raw.endswith("USDT") else raw
                    prices[bare] = float(price_str)
            return prices
        except Exception as e:
            logger.error("get_all_prices_failed", error=str(e))
            return {}

    # ─────────────────────── account ─────────────────────────────────────────

    def get_account_value(self) -> float | None:
        """Get wallet balance in USDT."""
        try:
            res = self._get(_EP_ACCOUNT, params={"asset": "USDT", "productGroup": _PRODUCT_GROUP})
            if self._ok(res) and "data" in res:
                data = res["data"]
                val = data.get("balance") or data.get("available")
                if val:
                    return float(val)
            logger.warning("get_account_value_empty", res=res)
            return None
        except Exception as e:
            logger.error("get_account_value_failed", error=str(e))
            return None

    # ─────────────────────── positions ───────────────────────────────────────

    def get_position(self, symbol: str) -> PositionInfo | None:
        """Get current open position for a symbol."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._get(_EP_POSITION, params={"productGroup": _PRODUCT_GROUP, "symbol": contract_symbol})
            positions = res if isinstance(res, list) else res.get("data", [])
            if not isinstance(positions, list):
                positions = []
            for pos in positions:
                size = float(pos.get("position", 0) or 0)
                if size == 0:
                    continue
                posi_dir = str(pos.get("posiDirection", ""))
                if posi_dir == "0":
                    side = "long"
                elif posi_dir == "1":
                    side = "short"
                else:
                    continue
                return PositionInfo(
                    symbol=symbol, side=side, size=size,
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
        """Get all open positions."""
        try:
            res = self._get(_EP_POSITION, params={"productGroup": _PRODUCT_GROUP})
            positions_raw = res if isinstance(res, list) else res.get("data", [])
            if not isinstance(positions_raw, list):
                return []
            result = []
            for pos in positions_raw:
                size = float(pos.get("position", 0) or 0)
                if size == 0:
                    continue
                posi_dir = str(pos.get("posiDirection", ""))
                if posi_dir == "0":
                    side = "long"
                elif posi_dir == "1":
                    side = "short"
                else:
                    continue
                raw = str(pos.get("symbol", ""))
                bare = raw.replace("USDT", "") if raw.endswith("USDT") else raw
                result.append(PositionInfo(
                    symbol=bare, side=side, size=size,
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

    # ─────────────────────── entry + SL + TP ─────────────────────────────────

    def place_market_order(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        sl_price: float | None = None,
        tp_price: float | None = None,
    ) -> OrderResult:
        """Open a position at market price with optional SL and TP attached.

        Uses placeStopProfitAndLossOrder (confirmed working via live test):
            direction=0 (long) / 1 (short)  — opening direction
            offsetFlag=0                      — open position with SL/TP

        If sl_price or tp_price are None, places a plain market order via
        placeOrder instead (used when SL/TP will be set separately).

        Endpoint: POST /cfd/openApi/v1/prv/placeStopProfitAndLossOrder
        """
        try:
            contract_symbol = self._contract_symbol(symbol)
            is_long = (side == TradeSide.LONG)

            if sl_price is not None or tp_price is not None:
                # Entry + SL + TP in one shot (confirmed working)
                body: dict[str, Any] = {
                    "exchangeID": _EXCHANGE_ID,
                    "instrumentID": contract_symbol,
                    "direction": "0" if is_long else "1",
                    "offsetFlag": "0",
                    "posiDirection": self._posi_direction(side),
                    "triggerOrderType": "2",
                    "triggerPriceType": "0",
                    "profitAndLossDirection": "0" if is_long else "1",
                    "price": "",
                    "volume": str(size),
                    "resultType": "ACK",
                }
                if sl_price is not None:
                    body["closeSLTriggerPrice"] = str(sl_price)
                    body["closeSLPrice"] = ""
                if tp_price is not None:
                    body["closeTPTriggerPrice"] = str(tp_price)
                    body["closeTPPrice"] = ""

                res = self._post(_EP_PLACE_SL_TP, body, is_trading=True)
            else:
                # Plain market order (no SL/TP)
                body = {
                    "symbol": contract_symbol,
                    "side": "BUY" if is_long else "SELL",
                    "offsetFlag": "0",
                    "orderPriceType": "4",
                    "origType": "0",
                    "posiDirection": self._posi_direction(side),
                    "volume": str(size),
                    "resultType": "ACK",
                }
                res = self._post(_EP_PLACE_ORDER, body, is_trading=True)

            if self._ok(res):
                data = res.get("data") or {}
                order_id = str(data.get("orderSysID") or data.get("orderId") or "")
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
                    sl_price=sl_price,
                    tp_price=tp_price,
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

    # ─────────────────────── update SL ───────────────────────────────────────

    def modify_stop_loss(
        self,
        symbol: str,
        side: TradeSide,
        size: float,
        new_trigger_price: float,
        old_order_id: str | None = None,  # kept for API compatibility, not used
    ) -> OrderResult:
        """Update SL on an existing position.

        Uses placeStopProfitAndLossPosition which modifies the SL/TP
        on the existing open position without opening a new one.

        Confirmed field mapping for longs:
            direction=1 (sell = closing direction), offsetFlag=1 (close)
        For shorts:
            direction=0 (buy = closing direction), offsetFlag=1 (close)

        Endpoint: POST /cfd/openApi/v1/prv/placeStopProfitAndLossPosition
        """
        try:
            contract_symbol = self._contract_symbol(symbol)
            is_long = (side == TradeSide.LONG)

            body: dict[str, Any] = {
                "exchangeID": _EXCHANGE_ID,
                "instrumentID": contract_symbol,
                "direction": "1" if is_long else "0",  # closing direction
                "offsetFlag": "1",                       # close position
                "posiDirection": self._posi_direction(side),
                "triggerPriceType": "0",                 # latest price
                "triggerPriceCalType": "0",              # by absolute price
                "sLTriggerPrice": str(new_trigger_price),
                "volume": str(size),
            }

            res = self._post(_EP_UPDATE_SL_TP, body, is_trading=True)

            if self._ok(res):
                logger.info(
                    "order_success",
                    operation="modify_stop_loss",
                    symbol=symbol,
                    new_sl=new_trigger_price,
                    size=size,
                )
                return OrderResult(success=True, order_id=None)
            else:
                error = res.get("msg") or str(res)
                logger.error("modify_stop_loss_failed", symbol=symbol, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error("modify_stop_loss_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    # ─────────────────────── close position ──────────────────────────────────

    def close_position(
        self,
        symbol: str,
        side: TradeSide | None = None,
        size: float | None = None,
    ) -> OrderResult:
        """Close an existing position at market price.

        side=None → close all (offsetFlag=5)
        size=None → close all of that side (offsetFlag=5)
        size set  → close specific quantity (offsetFlag=1)
        """
        try:
            contract_symbol = self._contract_symbol(symbol)

            if side is None:
                body: dict[str, Any] = {
                    "symbol": contract_symbol,
                    "side": "SELL",
                    "offsetFlag": "5",
                    "orderPriceType": "4",
                    "origType": "0",
                    "resultType": "ACK",
                }
            else:
                offset = "1" if size is not None else "5"
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

            res = self._post(_EP_PLACE_ORDER, body, is_trading=True)

            if self._ok(res):
                data = res.get("data") or {}
                order_id = str(data.get("orderId") or data.get("orderSysID") or "")
                avg_price_str = data.get("avgPrice")
                avg_price = float(avg_price_str) if avg_price_str else None
                logger.info(
                    "order_success",
                    operation="close_position",
                    order_id=order_id,
                    symbol=symbol,
                    side=side.value if side else "all",
                )
                return OrderResult(success=True, order_id=order_id, fill_price=avg_price, avg_price=avg_price)
            else:
                error = res.get("msg") or str(res)
                logger.error("close_position_failed", symbol=symbol, error=error)
                return OrderResult(success=False, error=error)

        except Exception as e:
            logger.error("close_position_failed", symbol=symbol, error=str(e))
            return OrderResult(success=False, error=str(e))

    # ─────────────────────── cancel ──────────────────────────────────────────

    def cancel_order(self, symbol: str, order_id: str, order_type: str = "price") -> bool:
        """Cancel a single order. order_type: 'price' or 'plan'."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            body: dict[str, Any] = {"symbol": contract_symbol, "orderType": order_type}
            if order_id:
                body["orderId"] = order_id
            res = self._post(_EP_CANCEL_ORDER, body, is_trading=True)
            ok = self._ok(res)
            if not ok:
                logger.warning("cancel_order_nok", symbol=symbol, order_id=order_id, msg=res.get("msg"))
            return ok
        except Exception as e:
            logger.error("cancel_order_failed", symbol=symbol, order_id=order_id, error=str(e))
            return False

    def cancel_all_orders(self, symbol: str) -> None:
        """Cancel all open regular and plan orders for a symbol."""
        self.cancel_order(symbol, "", order_type="price")
        self.cancel_order(symbol, "", order_type="plan")

    # ─────────────────────── open orders ─────────────────────────────────────

    def get_open_orders(self, symbol: str) -> list[dict]:
        """Get all unfilled orders for a symbol (status=4)."""
        try:
            contract_symbol = self._contract_symbol(symbol)
            res = self._get(_EP_OPEN_ORDERS, params={
                "productGroup": _PRODUCT_GROUP,
                "symbol": contract_symbol,
                "pageNo": "1",
                "pageSize": "100",
            })
            if self._ok(res):
                data = res.get("data", {})
                result_list = data.get("resultList", [])
                return [o for o in result_list if str(o.get("status", "")) == "4"]
            return []
        except Exception as e:
            logger.error("get_open_orders_failed", symbol=symbol, error=str(e))
            return []
