"""FastAPI webhook server for receiving TradingView alerts.

Exposes endpoints for:
- POST /webhook  — Receive and validate TradingView alert payloads
- GET  /health   — Health check for monitoring
- GET  /trades   — List active trades (debugging)
- GET  /trades/{trade_id} — Get a specific trade
- GET  /signals  — List all signals (debugging)
- GET  /signals/{signal_id} — Get a specific signal

Two payload paths:
- New (Phase 6): No SL/TP prices → creates a pending signal for brain evaluation
- Legacy: Has SL/TP prices → immediate trade execution (backward compat)

Usage:
    app = create_app(settings, database, exchange_client)
    # Run with uvicorn: uvicorn bridge.webhook:app
"""

from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app.logging_setup import get_logger
from app.models import (
    CloseReason,
    MarketType,  
    Signal,
    SignalStatus,
    Trade,
    TradeAction,
    TradeSide,
    TradeStage,
    WebhookPayload,
    
)

if TYPE_CHECKING:
    from app.config import Settings
    from app.database import TradeDatabase
    from app.exchange import HyperliquidClient

logger = get_logger("app.webhook")


def create_app(
    settings: Settings,
    database: TradeDatabase,
    exchange_client: HyperliquidClient | None = None,
    *,
    lifespan: Any | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: Application settings.
        database: Initialized trade database.
        exchange_client: Connected Hyperliquid client (None during testing).
        lifespan: Optional async context manager for startup/shutdown lifecycle.

    Returns:
        Configured FastAPI app instance.
    """
    app = FastAPI(
        title="TradingView-Hyperliquid Bridge",
        description="Webhook bridge for automated trade execution",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Store dependencies on app state for access in route handlers
    app.state.settings = settings
    app.state.db = database
    app.state.exchange = exchange_client

    # --- Health Check ---

    @app.get("/health")
    async def health_check() -> dict[str, Any]:
        """Health check endpoint for monitoring."""
        exchange_connected = exchange_client.is_connected if exchange_client else False
        active_count = await database.count_active_trades()

        return {
            "status": "ok",
            "exchange_connected": exchange_connected,
            "active_trades": active_count,
            "testnet": False,
        }

    # --- Webhook Endpoint ---

    @app.post("/webhook")
    async def receive_webhook(request: Request) -> JSONResponse:
        """Receive and process a TradingView webhook alert.

        Validates the secret, parses the payload, and dispatches
        the appropriate trade action.

        Expected JSON body:
        {
            "secret": "your_webhook_secret",
            "action": "entry_long" | "entry_short" | "close" | "close_all",
            "symbol": "BTC",
            "entry_price": "67000.50",    // required for entry_*
            "sl_price": "65000",          // required for entry_*
            "tp1_price": "70000",         // required for entry_*
            "tp2_price": "75000",         // required for entry_*
            "size_usd": "50.0"           // optional override
        }
        """
        # Parse raw JSON
        try:
            body = await request.json()
        except Exception as exc:
            logger.warning("webhook_invalid_json")
            raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

        # Validate payload structure
        try:
            payload = WebhookPayload(**body)
        except ValidationError as e:
            logger.warning("webhook_validation_failed", errors=str(e))
            raise HTTPException(
                status_code=422,
                detail=f"Invalid payload: {e}",
            ) from e

        # Authenticate
        if payload.secret != settings.webhook_secret:
            logger.warning("webhook_auth_failed", symbol=payload.symbol)
            raise HTTPException(status_code=401, detail="Invalid secret")

        # Deduplication — build a deterministic key from the payload content
        dedup_key = _build_dedup_key(payload)
        is_duplicate = await database.check_and_store_dedup(dedup_key)
        if is_duplicate:
            logger.info(
                "webhook_duplicate_ignored",
                action=payload.action.value,
                symbol=payload.symbol,
                dedup_key=dedup_key,
            )
            return JSONResponse(
                content={"status": "ignored", "reason": "Duplicate webhook"},
                status_code=200,
            )

        logger.info(
            "webhook_received",
            action=payload.action.value,
            symbol=payload.symbol,
        )

        # Dispatch based on action
        try:
            if payload.action in (TradeAction.ENTRY_LONG, TradeAction.ENTRY_SHORT):
                side = (
                    TradeSide.LONG if payload.action == TradeAction.ENTRY_LONG else TradeSide.SHORT
                )
                # Detect payload format: has SL/TP -> legacy, no SL/TP -> signal
                if payload.sl_price:
                    result = await _handle_entry_legacy(
                        payload, side, settings, database, exchange_client
                    )
                else:
                    result = await _handle_entry_signal(payload, settings, database)
            elif payload.action == TradeAction.CLOSE:
                result = await _handle_close(payload, settings, database, exchange_client)
            elif payload.action == TradeAction.CLOSE_ALL:
                result = await _handle_close_all(payload, settings, database, exchange_client)
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown action: {payload.action}",
                )

            return JSONResponse(content=result, status_code=200)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                "webhook_processing_failed",
                action=payload.action.value,
                symbol=payload.symbol,
                error=str(e),
            )
            raise HTTPException(
                status_code=500,
                detail=f"Internal error processing webhook: {e}",
            ) from e

    # --- Trade List Endpoints ---

    @app.get("/trades")
    async def list_trades() -> list[dict[str, Any]]:
        """List all active trades for debugging."""
        trades = await database.get_active_trades()
        return [_trade_to_dict(t) for t in trades]

    @app.get("/trades/{trade_id}")
    async def get_trade(trade_id: str) -> dict[str, Any]:
        """Get a specific trade by ID."""
        trade = await database.get_trade(trade_id)
        if trade is None:
            raise HTTPException(status_code=404, detail="Trade not found")
        return _trade_to_dict(trade)

    # --- Signal Endpoints ---

    @app.get("/signals")
    async def list_signals() -> list[dict[str, Any]]:
        """List all signals for debugging."""
        signals = await database.get_all_signals()
        return [_signal_to_dict(s) for s in signals]

    @app.get("/signals/{signal_id}")
    async def get_signal(signal_id: str) -> dict[str, Any]:
        """Get a specific signal by ID."""
        sig = await database.get_signal(signal_id)
        if sig is None:
            raise HTTPException(status_code=404, detail="Signal not found")
        return _signal_to_dict(sig)

    # --- Snapshot Endpoints ---

    @app.get("/snapshots")
    async def list_snapshots(limit: int = 30) -> list[dict[str, Any]]:
        """List recent account balance snapshots (newest first)."""
        snapshots = await database.get_snapshots(limit=limit)
        return [s.model_dump(mode="json") for s in snapshots]

    return app


# --- Action Handlers ---


async def _handle_entry_signal(
    payload: WebhookPayload,
    settings: Settings,
    database: TradeDatabase,
) -> dict[str, Any]:
    """Handle a new-format entry webhook (no SL/TP prices).

    Creates a pending signal for the Brain to evaluate after the
    configured delay. The monitor loop picks these up.

    Args:
        payload: Validated webhook payload (no SL/TP).
        settings: Application settings.
        database: Trade database.

    Returns:
        Response dict with signal ID.
    """
    entry_price = payload.get_entry_price()
    if not entry_price:
        return {"status": "error", "reason": "Missing entry_price for signal"}

    # --- Layer 1: Payload-level stale TTL check ---
    if payload.timestamp:
        try:
            signal_time = float(payload.timestamp)
            if time.time() - signal_time > 3600:
                logger.info(
                    "signal_stale_ttl_payload",
                    symbol=payload.symbol,
                    age_seconds=int(time.time() - signal_time),
                )
                return {"status": "ignored", "reason": "stale_signal_ttl_expired"}
        except (ValueError, TypeError):
            pass  # Invalid timestamp format — ignore, proceed normally

    # --- Layer 2: Symbol-Action Lock (30s dedup window) ---
    cutoff = datetime.now(UTC) - timedelta(seconds=30)
    recent = await database.get_recent_signal(
        payload.symbol, payload.action.value, cutoff
    )
    if recent:
        logger.info(
            "signal_symbol_action_locked",
            symbol=payload.symbol,
            action=payload.action.value,
            existing_signal=recent.id,
        )
        return {"status": "ignored", "reason": "symbol_action_lock"}

    # --- Cluster countdown reset ---
    existing_pending = await database.get_pending_signals_by_symbol(payload.symbol)
    # Only reset if same action (long stays long, short stays short)
    same_action_pending = [s for s in existing_pending if s.action == payload.action]
    if same_action_pending:
        # Reset the countdown on the existing signal instead of creating a duplicate
        existing = same_action_pending[0]
        existing.created_at = datetime.now(UTC)
        existing.entry_price = entry_price
        await database.update_signal(existing)

        logger.info(
            "signal_countdown_reset",
            signal_id=existing.id,
            symbol=existing.symbol,
            new_entry_price=entry_price,
        )

        return {
            "status": "ok",
            "signal_id": existing.id,
            "countdown_reset": True,
            "symbol": existing.symbol,
        }

    size_usd = payload.get_size_usd() or settings.default_trade_size_usd
    
    # Derive market_type from payload or settings
    market_type = payload.market_type or (
        MarketType.SPOT
        if settings.use_spot_for_longs and payload.action == TradeAction.ENTRY_LONG
        else MarketType.PERP
    )

    signal = Signal(
        symbol=payload.symbol,
        action=payload.action,
        signal_type=payload.signal_type or "liquidation_bubble",
        entry_price=entry_price,
        size_usd=size_usd,
        status=SignalStatus.PENDING_EVAL,
        market_type=market_type,
    )

    await database.save_signal(signal)

    logger.info(
        "signal_created",
        signal_id=signal.id,
        symbol=signal.symbol,
        action=signal.action.value,
        signal_type=signal.signal_type,
        entry_price=entry_price,
        size_usd=size_usd,
        market_type=market_type,
    )

    return {
        "status": "ok",
        "signal_id": signal.id,
        "symbol": signal.symbol,
        "action": signal.action.value,
        "signal_type": signal.signal_type,
        "eval_delay_minutes": settings.signal_eval_delay_minutes,
    }


async def _handle_entry_legacy(
    payload: WebhookPayload,
    side: TradeSide,
    settings: Settings,
    database: TradeDatabase,
    exchange_client: HyperliquidClient | None,
) -> dict[str, Any]:
    """Handle a legacy entry webhook (with SL/TP prices).

    Backward-compatible path for payloads that include explicit
    SL/TP prices from TradingView.

    1. Check for existing active trades on this symbol
    2. Calculate order size
    3. Place market order via exchange (spot for longs, perp for shorts)
    4. Create and persist the Trade record

    Args:
        payload: Validated webhook payload with SL/TP.
        side: LONG or SHORT.
        settings: Application settings.
        database: Trade database.
        exchange_client: Hyperliquid client.

    Returns:
        Response dict with trade details.
    """
    symbol = payload.symbol

    # Check for existing active trades on this symbol
    existing = await database.get_trades_by_symbol(symbol)
    if existing:
        logger.warning(
            "entry_rejected_existing_trade",
            symbol=symbol,
            existing_count=len(existing),
        )
        return {
            "status": "rejected",
            "reason": f"Active trade already exists for {symbol}",
            "existing_trade_id": existing[0].id,
        }

    # Extract prices from payload
    entry_price = payload.get_entry_price()
    sl_price = payload.get_sl_price()
    tp1_price = payload.get_tp1_price()
    tp2_price = payload.get_tp2_price()

    if not all([entry_price, sl_price, tp1_price, tp2_price]):
        return {
            "status": "error",
            "reason": "Missing required price fields for entry action",
        }

    # Derive market_type from payload or settings
    market_type = payload.market_type or (
        MarketType.SPOT
        if settings.use_spot_for_longs and side == TradeSide.LONG
        else MarketType.PERP
    )

    # Determine trade size
    size_usd = payload.get_size_usd() or settings.default_trade_size_usd
    quantity: float | None = None
    order_id: str | None = None
    fill_price: float | None = None
    sl_order_id: str | None = None
    tp1_order_id: str | None = None
    tp2_order_id: str | None = None

    if exchange_client and exchange_client.is_connected:
        # Calculate size in contracts
        quantity = exchange_client.calculate_order_size(symbol, size_usd)
        if quantity is None:
            return {
                "status": "error",
                "reason": f"Could not calculate order size for {symbol}",
            }

        # Place market order — route by market type
        if market_type == MarketType.SPOT:
            result = exchange_client.place_spot_market_order(symbol, True, quantity)
        else:
            result = exchange_client.place_market_order(symbol, side, quantity)

        if not result.success:
            return {
                "status": "error",
                "reason": f"Order failed: {result.error}",
            }

        order_id = result.order_id
        fill_price = result.avg_price or entry_price

        # Update entry price to actual fill if available
        if result.avg_price:
            entry_price = result.avg_price

        tp1_size = round(quantity * settings.partial_exit_fraction, 8)
        tp2_size = round(quantity - tp1_size, 8)

        # Place SL + TP — route by market type
        if market_type == MarketType.SPOT:
            sl_result = exchange_client.place_spot_stop_loss(symbol, quantity, sl_price)
            tp1_result = exchange_client.place_spot_take_profit(symbol, tp1_size, tp1_price)
            tp2_result = exchange_client.place_spot_take_profit(symbol, tp2_size, tp2_price)
        else:
            sl_result = exchange_client.place_stop_loss(symbol, side, quantity, sl_price)
            tp1_result = exchange_client.place_take_profit(symbol, side, tp1_size, tp1_price)
            tp2_result = exchange_client.place_take_profit(symbol, side, tp2_size, tp2_price)

        sl_order_id = sl_result.order_id if sl_result.success else None
        tp1_order_id = tp1_result.order_id if tp1_result.success else None
        tp2_order_id = tp2_result.order_id if tp2_result.success else None
    else:
        # No exchange client (testing mode) — use prices from webhook
        quantity = size_usd / entry_price if entry_price > 0 else 0.001
        fill_price = entry_price
        logger.warning("exchange_not_connected_paper_trade", symbol=symbol)

    # Create the trade record
    trade = Trade(
        symbol=symbol,
        side=side,
        stage=TradeStage.ENTRY,
        entry_price=fill_price,
        sl_price=sl_price,
        tp1_price=tp1_price,
        tp2_price=tp2_price,
        quantity=quantity,
        entry_order_id=order_id,
        sl_order_id=sl_order_id,
        tp1_order_id=tp1_order_id,
        tp2_order_id=tp2_order_id,
        market_type=market_type,
    )

    await database.save_trade(trade)

    logger.info(
        "trade_opened",
        trade_id=trade.id,
        symbol=symbol,
        side=side.value,
        market_type=market_type.value,
        entry_price=fill_price,
        quantity=quantity,
        sl_price=sl_price,
    )

    return {
        "status": "ok",
        "trade_id": trade.id,
        "symbol": symbol,
        "side": side.value,
        "market_type": market_type.value,
        "entry_price": fill_price,
        "quantity": quantity,
        "sl_price": sl_price,
        "tp1_price": tp1_price,
        "tp2_price": tp2_price,
    }


async def _handle_close(
    payload: WebhookPayload,
    settings: Settings,
    database: TradeDatabase,
    exchange_client: HyperliquidClient | None,
) -> dict[str, Any]:
    """Handle a close webhook action for a specific symbol.

    Closes all active trades for the given symbol.

    Args:
        payload: Validated webhook payload.
        settings: Application settings.
        database: Trade database.
        exchange_client: Hyperliquid client.

    Returns:
        Response dict with close results.
    """
    symbol = payload.symbol
    trades = await database.get_trades_by_symbol(symbol)

    if not trades:
        return {
            "status": "ok",
            "reason": f"No active trades for {symbol}",
            "closed_count": 0,
        }

    closed_ids = []
    for trade in trades:
        # Close on exchange
        if exchange_client and exchange_client.is_connected:
            exchange_client.cancel_all_orders(symbol)
            if trade.market_type == MarketType.SPOT:
                exchange_client.close_spot_position(symbol, trade.remaining_quantity)
            else:
                exchange_client.close_position(symbol)

        # Update trade in database
        trade.stage = TradeStage.CLOSED
        trade.closed_at = datetime.now(UTC)
        trade.close_reason = CloseReason.WEBHOOK_CLOSE
        await database.update_trade(trade)
        closed_ids.append(trade.id)

    logger.info("trades_closed_by_webhook", symbol=symbol, count=len(closed_ids))

    return {
        "status": "ok",
        "symbol": symbol,
        "closed_count": len(closed_ids),
        "closed_trade_ids": closed_ids,
    }


async def _handle_close_all(
    payload: WebhookPayload,
    settings: Settings,
    database: TradeDatabase,
    exchange_client: HyperliquidClient | None,
) -> dict[str, Any]:
    """Handle a close_all webhook action.

    Closes ALL active trades across all symbols.

    Args:
        payload: Validated webhook payload.
        settings: Application settings.
        database: Trade database.
        exchange_client: Hyperliquid client.

    Returns:
        Response dict with close results.
    """
    trades = await database.get_active_trades()

    if not trades:
        return {
            "status": "ok",
            "reason": "No active trades to close",
            "closed_count": 0,
        }

    closed_ids = []
    symbols_closed = set()

    for trade in trades:
        if exchange_client and exchange_client.is_connected and trade.symbol not in symbols_closed:
            exchange_client.cancel_all_orders(trade.symbol)
            if trade.market_type == MarketType.SPOT:
                exchange_client.close_spot_position(trade.symbol, trade.remaining_quantity)
            else:
                exchange_client.close_position(trade.symbol)
           
            symbols_closed.add(trade.symbol)

        trade.stage = TradeStage.CLOSED
        trade.closed_at = datetime.now(UTC)
        trade.close_reason = CloseReason.WEBHOOK_CLOSE
        await database.update_trade(trade)
        closed_ids.append(trade.id)

    logger.info(
        "all_trades_closed_by_webhook",
        count=len(closed_ids),
        symbols=list(symbols_closed),
    )

    return {
        "status": "ok",
        "closed_count": len(closed_ids),
        "closed_trade_ids": closed_ids,
        "symbols": list(symbols_closed),
    }


# --- Helpers ---


def _build_dedup_key(payload: WebhookPayload) -> str:
    """Build a deterministic dedup key from webhook payload.

    Combines the action, symbol, and all price fields into a SHA-256 hash.
    Two identical alerts from TradingView will produce the same key,
    preventing double-processing within the TTL window.
    """
    parts = [
        payload.action.value,
        payload.symbol,
        payload.entry_price or "",
        payload.sl_price or "",
        payload.tp1_price or "",
        payload.tp2_price or "",
        payload.size_usd or "",
        payload.signal_type or "",
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _trade_to_dict(trade: Trade) -> dict[str, Any]:
    """Convert a Trade model to a JSON-friendly dict."""
    return {
        "id": trade.id,
        "symbol": trade.symbol,
        "side": trade.side.value,
        "stage": trade.stage.value,
        "entry_price": trade.entry_price,
        "sl_price": trade.sl_price,
        "tp1_price": trade.tp1_price,
        "tp2_price": trade.tp2_price,
        "quantity": trade.quantity,
        "remaining_quantity": trade.remaining_quantity,
        "be_triggered": trade.be_triggered,
        "partial_exit_done": trade.partial_exit_done,
        "trailing_active": trade.trailing_active,
        "is_active": trade.is_active,
        "created_at": trade.created_at.isoformat(),
        "updated_at": trade.updated_at.isoformat(),
        "closed_at": trade.closed_at.isoformat() if trade.closed_at else None,
        "close_reason": trade.close_reason.value if trade.close_reason else None,
        "signal_id": trade.signal_id,
        "market_type": trade.market_type.value,
    }


def _signal_to_dict(signal: Signal) -> dict[str, Any]:
    """Convert a Signal model to a JSON-friendly dict."""
    return {
        "id": signal.id,
        "symbol": signal.symbol,
        "action": signal.action.value,
        "signal_type": signal.signal_type,
        "entry_price": signal.entry_price,
        "size_usd": signal.size_usd,
        "status": signal.status.value,
        "ema_slope_value": signal.ema_slope_value,
        "delta_slope_value": signal.delta_slope_value,
        "eval_price": signal.eval_price,
        "actual_size_usd": signal.actual_size_usd,
        "rejection_reason": signal.rejection_reason,
        "trade_id": signal.trade_id,
        "ema_slope_history": signal.ema_slope_history,
        "delta_slope_history": signal.delta_slope_history,
        "created_at": signal.created_at.isoformat(),
        "evaluated_at": signal.evaluated_at.isoformat() if signal.evaluated_at else None,
    }
