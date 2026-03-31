"""Trade lifecycle stage evaluator.

Pure state machine that evaluates whether a trade should advance to its
next stage based on the current mark price. Each stage transition triggers
specific exchange actions (move SL, partial close, trailing ratchet, etc).

The evaluate_trade() function is the single entry point called by the
monitor loop on every tick for every active trade. It enforces the
"exactly one transition per tick" invariant by returning after the first
applicable transition.

Stage flow:
    ENTRY → BREAKEVEN → PARTIAL_EXIT → TRAILING_ACTIVE → TRAILING_UPDATE → CLOSED

Usage:
    result = evaluate_trade(trade, mark_price, settings, exchange)
    if result.transitioned:
        await db.update_trade_stage(trade, result.new_stage)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.logging_setup import get_logger
from app.models import CloseReason, TradeSide, TradeStage

if TYPE_CHECKING:
    from app.config import Settings
    from app.exchange import LBankClient
    from app.models import Trade

logger = get_logger("app.lifecycle")


@dataclass
class StageResult:
    """Result of a stage evaluation attempt."""

    transitioned: bool = False
    new_stage: TradeStage | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check_trigger(
    mark_price: float,
    entry_price: float,
    threshold_percent: float,
    side: TradeSide,
) -> bool:
    """Check if mark_price has crossed a trigger threshold.

    Config percentages are stored as human-readable values (0.5 = 0.5%),
    so we divide by 100 to get the multiplier (0.005).

    For LONG:  mark_price >= entry_price * (1 + threshold / 100)
    For SHORT: mark_price <= entry_price * (1 - threshold / 100)
    """
    multiplier = threshold_percent / 100
    if side == TradeSide.LONG:
        trigger_price = entry_price * (1 + multiplier)
        return mark_price >= trigger_price
    # SHORT
    trigger_price = entry_price * (1 - multiplier)
    return mark_price <= trigger_price


def _move_stop_loss(
    trade: Trade,
    new_sl_price: float,
    exchange: LBankClient | None,
    sl_max_retries: int = 3,
) -> bool:
    """Place new SL, then cancel old one. Mutates trade in-place.

    Uses trade.remaining_quantity for the SL size so the order
    covers only the current position (important after partial exits).

    Returns True if SL was successfully placed (or no exchange in paper mode).
    Returns False if placement failed after retries — trade keeps old SL.
    """
    if not exchange or not exchange.is_connected:
        # Paper mode — just update the price
        trade.sl_price = new_sl_price
        return True

    old_order_id = trade.sl_order_id or None

    for attempt in range(1, sl_max_retries + 1):
        result = exchange.modify_stop_loss(
            symbol=trade.symbol,
            side=trade.side,
            size=trade.remaining_quantity,
            new_trigger_price=new_sl_price,
            old_order_id=old_order_id,
        )

        if result.success:
            old_sl = trade.sl_price
            trade.sl_order_id = result.order_id
            trade.sl_price = new_sl_price
            logger.info(
                "sl_move_audit",
                trade_id=trade.id,
                symbol=trade.symbol,
                old_sl=old_sl,
                new_sl=new_sl_price,
                new_order_id=result.order_id,
                size=trade.remaining_quantity,
            )
            return True

        logger.warning(
            "move_sl_attempt_failed",
            trade_id=trade.id,
            attempt=attempt,
            max_attempts=sl_max_retries,
            new_sl=new_sl_price,
            error=result.error,
        )

    # All retries exhausted — old SL stays, trade.sl_price unchanged
    logger.error(
        "move_sl_failed_all_retries",
        trade_id=trade.id,
        new_sl=new_sl_price,
        old_sl=trade.sl_price,
        attempts=sl_max_retries,
    )
    return False


def close_trade(
    trade: Trade,
    reason: CloseReason,
    close_price: float | None = None,
) -> None:
    """Mark a trade as CLOSED. Mutates trade in-place; caller persists.

    Calculates P&L from entry_price, close_price (or tp1_fill_price
    for partial close leg), and position quantities.

    Args:
        trade: The trade to close.
        reason: Why the trade is being closed.
        close_price: Mark price at time of close (for P&L calculation).
    """
    trade.stage = TradeStage.CLOSED
    trade.closed_at = datetime.now(UTC)
    trade.close_reason = reason
    if close_price is not None:
        trade.close_price = close_price

    if close_price is not None and trade.entry_price > 0:
        direction = 1.0 if trade.side == TradeSide.LONG else -1.0
        remaining_pnl = direction * (close_price - trade.entry_price) * trade.remaining_quantity
        tp1_pnl = 0.0
        if trade.partial_exit_done and trade.tp1_fill_price is not None:
            closed_qty = trade.quantity - trade.remaining_quantity
            tp1_pnl = direction * (trade.tp1_fill_price - trade.entry_price) * closed_qty
        trade.pnl_usd = round(remaining_pnl + tp1_pnl, 2)
        entry_notional = trade.entry_price * trade.quantity
        if entry_notional > 0:
            trade.pnl_pct = round((trade.pnl_usd / entry_notional) * 100, 4)

    logger.info(
        "trade_audit_close",
        trade_id=trade.id,
        symbol=trade.symbol,
        side=trade.side.value,
        reason=reason.value,
        entry_price=trade.entry_price,
        close_price=close_price,
        quantity=trade.quantity,
        remaining_quantity=trade.remaining_quantity,
        pnl_usd=trade.pnl_usd,
        pnl_pct=trade.pnl_pct,
        tp1_fill_price=trade.tp1_fill_price,
        partial_exit_done=trade.partial_exit_done,
    )


# ---------------------------------------------------------------------------
# Stage Handlers
# ---------------------------------------------------------------------------


def _handle_entry(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> StageResult:
    """ENTRY → BREAKEVEN: Move SL to true breakeven (entry + round-trip fees).

    LONG:  mark >= entry * (1 + be_trigger_pct/100)
    SHORT: mark <= entry * (1 - be_trigger_pct/100)

    True breakeven accounts for round-trip exchange fees (entry + exit):
    LONG:  true_be = entry * (1 + (fee_pct * 2) / 100)
    SHORT: true_be = entry * (1 - (fee_pct * 2) / 100)
    """
    if not _check_trigger(mark_price, trade.entry_price, settings.trade_be_trigger_pct, trade.side):
        return StageResult()

    fee_mult = (settings.exchange_fee_pct * 2) / 100
    if trade.side == TradeSide.LONG:
        true_be = trade.entry_price * (1 + fee_mult)
    else:
        true_be = trade.entry_price * (1 - fee_mult)

    if not _move_stop_loss(trade, true_be, exchange, settings.sl_max_retries):
        return StageResult(error="SL placement failed at breakeven — keeping old SL")

    trade.be_triggered = True

    logger.info(
        "breakeven_triggered",
        trade_id=trade.id,
        symbol=trade.symbol,
        mark_price=mark_price,
        new_sl=true_be,
    )
    return StageResult(transitioned=True, new_stage=TradeStage.BREAKEVEN)


def _handle_breakeven(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> StageResult:
    """BREAKEVEN → PARTIAL_EXIT: Close partial_exit_fraction of position at TP1.

    LONG:  mark >= entry * (1 + tp1_trigger_pct/100)
    SHORT: mark <= entry * (1 - tp1_trigger_pct/100)
    """
    if not _check_trigger(
        mark_price,
        trade.entry_price,
        settings.trade_tp1_trigger_pct,
        trade.side,
    ):
        return StageResult()

    partial_size = round(trade.quantity * settings.partial_exit_fraction, 8)

    # Size floor: ensure partial close notional meets minimum
    min_notional = settings.min_close_notional_usd
    partial_notional = partial_size * mark_price
    if partial_notional < min_notional and mark_price > 0:
        min_size = min_notional / mark_price
        partial_size = min(round(min_size, 8), trade.quantity)
        logger.info(
            "partial_size_floor_applied",
            trade_id=trade.id,
            original_notional=round(partial_notional, 2),
            adjusted_size=partial_size,
            adjusted_notional=round(partial_size * mark_price, 2),
        )

    if exchange and exchange.is_connected:
        # Check actual position size to guard against TP1 exchange order race
        position = exchange.get_position(trade.symbol)
        actual_size = abs(position.size) if position else 0.0

        expected_remaining = round(trade.quantity - partial_size, 8)

        if actual_size <= expected_remaining + 1e-9:
            # TP1 already filled on exchange — skip market close
            logger.info(
                "tp1_already_filled_on_exchange",
                trade_id=trade.id,
                symbol=trade.symbol,
                actual_size=actual_size,
                expected_remaining=expected_remaining,
            )
        else:
            close_result = exchange.close_position(
                trade.symbol, trade.side, size=partial_size
            )
            if not close_result.success:
                logger.error(
                    "partial_close_failed",
                    trade_id=trade.id,
                    error=close_result.error,
                )
                return StageResult(error=close_result.error)

        # Cancel TP1 exchange order — may already be filled, that's fine
        if trade.tp1_order_id:
            exchange.cancel_order(trade.symbol, trade.tp1_order_id, order_type="plan")

    # Update trade state
    trade.remaining_quantity = round(trade.quantity - partial_size, 8)
    trade.partial_exit_done = True
    trade.tp1_fill_price = mark_price
    trade.tp1_order_id = None

    # Re-place SL with remaining_quantity (old SL has wrong size after partial close)
    sl_ok = _move_stop_loss(trade, trade.sl_price, exchange, settings.sl_max_retries)

    logger.info(
        "partial_exit_done",
        trade_id=trade.id,
        symbol=trade.symbol,
        closed_size=partial_size,
        remaining=trade.remaining_quantity,
    )

    error = None if sl_ok else "SL re-placement failed after partial exit — trade may have no SL"
    return StageResult(transitioned=True, new_stage=TradeStage.PARTIAL_EXIT, error=error)


def _handle_partial_exit(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> StageResult:
    """PARTIAL_EXIT → TRAILING_ACTIVE: Activate trailing stop.

    LONG:  mark >= entry * (1 + trail_active_pct/100)
    SHORT: mark <= entry * (1 - trail_active_pct/100)
    """
    if not _check_trigger(
        mark_price,
        trade.entry_price,
        settings.trade_trail_active_pct,
        trade.side,
    ):
        return StageResult()

    # Cancel TP2 exchange order — trailing stop takes over from here
    if exchange and exchange.is_connected and trade.tp2_order_id:
        exchange.cancel_order(trade.symbol, trade.tp2_order_id, order_type="plan")
    trade.tp2_order_id = None

    # Compute trailing offset and initial trailing SL
    offset_pct = settings.trade_trail_lock_pct / 100

    if trade.side == TradeSide.LONG:
        initial_sl = trade.entry_price * (1 + offset_pct)
        trade.highest_price = mark_price
    else:
        initial_sl = trade.entry_price * (1 - offset_pct)
        trade.lowest_price = mark_price

    trade.trailing_active = True
    trade.trailing_offset = trade.entry_price * offset_pct

    if not _move_stop_loss(trade, initial_sl, exchange, settings.sl_max_retries):
        return StageResult(error="SL placement failed at trailing activation — keeping old SL")

    logger.info(
        "trailing_activated",
        trade_id=trade.id,
        symbol=trade.symbol,
        initial_sl=initial_sl,
        watermark=mark_price,
    )
    return StageResult(transitioned=True, new_stage=TradeStage.TRAILING_ACTIVE)


def _update_trailing_stop(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> None:
    """Ratchet trailing SL to follow new highs (longs) or new lows (shorts).

    SL never moves backward — only tightens as price moves in our favour.

    LONG:  new high → candidate_sl = high * (1 - trail_lock_pct/100). Move if > current SL.
    SHORT: new low  → candidate_sl = low  * (1 + trail_lock_pct/100). Move if < current SL.
    """
    offset_pct = settings.trade_trail_lock_pct / 100

    if trade.side == TradeSide.LONG:
        if mark_price > trade.highest_price:
            old_watermark = trade.highest_price
            trade.highest_price = mark_price
            candidate_sl = trade.highest_price * (1 - offset_pct)
            if candidate_sl > trade.sl_price:
                if not _move_stop_loss(trade, candidate_sl, exchange, settings.sl_max_retries):
                    trade.highest_price = old_watermark  # Revert so next tick retries
                else:
                    logger.debug(
                        "trailing_sl_ratcheted",
                        trade_id=trade.id,
                        new_sl=candidate_sl,
                        watermark=trade.highest_price,
                    )
    else:  # SHORT
        if mark_price < trade.lowest_price:
            old_watermark = trade.lowest_price
            trade.lowest_price = mark_price
            candidate_sl = trade.lowest_price * (1 + offset_pct)
            if candidate_sl < trade.sl_price:
                if not _move_stop_loss(trade, candidate_sl, exchange, settings.sl_max_retries):
                    trade.lowest_price = old_watermark  # Revert so next tick retries
                else:
                    logger.debug(
                        "trailing_sl_ratcheted",
                        trade_id=trade.id,
                        new_sl=candidate_sl,
                        watermark=trade.lowest_price,
                    )


def _check_tp2_exit(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> StageResult | None:
    """Check if price hit TP2 hard exit level.

    Closes the remaining position when price reaches the TP2 trigger.
    Checked before the trailing ratchet in both trailing stages.

    Returns:
        StageResult for CLOSED if TP2 hit, None otherwise.
    """
    if not _check_trigger(
        mark_price, trade.entry_price, settings.trade_tp2_trigger_pct, trade.side
    ):
        return None

    if exchange and exchange.is_connected:
        # Check whether TP2 exchange order already filled
        position = exchange.get_position(trade.symbol)
        actual_size = abs(position.size) if position else 0.0

        if actual_size > 1e-9:
            exchange.close_position(trade.symbol, trade.side, size=trade.remaining_quantity)
        else:
            logger.info(
                "tp2_already_filled_on_exchange",
                trade_id=trade.id,
                symbol=trade.symbol,
            )

        exchange.cancel_all_orders(trade.symbol)

    close_trade(trade, CloseReason.TAKE_PROFIT, close_price=mark_price)

    logger.info(
        "tp2_hard_exit",
        trade_id=trade.id,
        symbol=trade.symbol,
        mark_price=mark_price,
        entry_price=trade.entry_price,
        tp2_pct=settings.trade_tp2_trigger_pct,
    )
    return StageResult(transitioned=True, new_stage=TradeStage.CLOSED)


def _handle_trailing_active(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> StageResult:
    """TRAILING_ACTIVE → TRAILING_UPDATE (or CLOSED via TP2)."""
    tp2_result = _check_tp2_exit(trade, mark_price, settings, exchange)
    if tp2_result:
        return tp2_result

    _update_trailing_stop(trade, mark_price, settings, exchange)
    return StageResult(transitioned=True, new_stage=TradeStage.TRAILING_UPDATE)


def _handle_trailing_update(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> StageResult:
    """TRAILING_UPDATE → TRAILING_UPDATE (or CLOSED via TP2).

    Always returns transitioned=True so the caller persists the updated
    watermark and SL fields even when no SL modification occurred.
    """
    tp2_result = _check_tp2_exit(trade, mark_price, settings, exchange)
    if tp2_result:
        return tp2_result

    _update_trailing_stop(trade, mark_price, settings, exchange)
    return StageResult(transitioned=True, new_stage=TradeStage.TRAILING_UPDATE)


# ---------------------------------------------------------------------------
# Main Dispatcher
# ---------------------------------------------------------------------------

_STAGE_HANDLERS = {
    TradeStage.ENTRY: _handle_entry,
    TradeStage.BREAKEVEN: _handle_breakeven,
    TradeStage.PARTIAL_EXIT: _handle_partial_exit,
    TradeStage.TRAILING_ACTIVE: _handle_trailing_active,
    TradeStage.TRAILING_UPDATE: _handle_trailing_update,
}


def evaluate_trade(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> StageResult:
    """Evaluate a single trade against the current mark price.

    Dispatches to the handler for the trade's current stage. Returns
    after at most ONE stage transition (no leapfrogging).

    The trade object is mutated in-place. The caller is responsible
    for persisting the changes.
    """
    handler = _STAGE_HANDLERS.get(trade.stage)
    if handler is None:
        return StageResult()

    return handler(trade, mark_price, settings, exchange)
