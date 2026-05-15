"""Trade lifecycle stage evaluator.

Pure state machine that evaluates whether a trade should advance to its
next stage based on the current mark price. Each stage transition triggers
specific exchange actions (move SL, partial close, trailing ratchet, etc).

LBank order strategy:
    - Entry + initial SL + TP2 placed together via place_market_order
    - TP1 partial exit handled entirely in software by this module
      (no TP1 exchange order — monitor closes 50% via close_position)
    - SL updates use modify_stop_loss (placeStopProfitAndLossPosition)
    - No cancel_order calls needed — no separate SL/TP orders to cancel

Stage flow:
    ENTRY → BREAKEVEN → PARTIAL_EXIT → TRAILING_ACTIVE → TRAILING_UPDATE → CLOSED
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

    For LONG:  mark_price >= entry_price * (1 + threshold / 100)
    For SHORT: mark_price <= entry_price * (1 - threshold / 100)
    """
    multiplier = threshold_percent / 100
    if side == TradeSide.LONG:
        return mark_price >= entry_price * (1 + multiplier)
    return mark_price <= entry_price * (1 - multiplier)


def _move_stop_loss(
    trade: Trade,
    new_sl_price: float,
    exchange: LBankClient | None,
    sl_max_retries: int = 3,
) -> bool:
    """Update SL on the exchange position. Mutates trade in-place.

    Uses exchange.modify_stop_loss which calls placeStopProfitAndLossPosition
    to update the SL on the existing open position.

    Returns True if successful (or paper mode). False if all retries fail.
    """
    if not exchange or not exchange.is_connected:
        trade.sl_price = new_sl_price
        return True

    for attempt in range(1, sl_max_retries + 1):
        result = exchange.modify_stop_loss(
            symbol=trade.symbol,
            side=trade.side,
            size=trade.remaining_quantity,
            new_trigger_price=new_sl_price,
            trade_unit_id=trade.trade_unit_id,
        )

        if result.success:
            old_sl = trade.sl_price
            trade.sl_price = new_sl_price
            # sl_order_id not used with position-level SL updates
            logger.info(
                "sl_move_audit",
                trade_id=trade.id,
                symbol=trade.symbol,
                old_sl=old_sl,
                new_sl=new_sl_price,
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
    """Mark a trade as CLOSED and calculate P&L. Mutates trade in-place."""
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
    """ENTRY → BREAKEVEN: Move SL to true breakeven when price hits BE trigger.

    True breakeven = entry + round-trip fees so we don't lose on the trade.
    LONG:  true_be = entry * (1 + fee_pct*2/100)
    SHORT: true_be = entry * (1 - fee_pct*2/100)
    """
    # ── DD BE: move TP1 to entry+fees when drawdown hits ─────────────────
    if settings.dd_be_enabled and not trade.dd_be_triggered and not trade.be_triggered:
        direction = 1 if trade.side == TradeSide.LONG else -1
        dd_hit = (
            (trade.side == TradeSide.LONG  and mark_price <= trade.entry_price * (1 - settings.dd_be_trigger_pct / 100)) or
            (trade.side == TradeSide.SHORT and mark_price >= trade.entry_price * (1 + settings.dd_be_trigger_pct / 100))
        )
        if dd_hit:
            fee_mult_dd = (settings.exchange_fee_pct * 2) / 100
            if trade.side == TradeSide.LONG:
                be_price = round(trade.entry_price * (1 + fee_mult_dd), 2)
            else:
                be_price = round(trade.entry_price * (1 - fee_mult_dd), 2)

            if exchange and exchange.is_connected:
                
                exchange.modify_stop_loss(
                    symbol=trade.symbol,
                    side=trade.side,
                    size=trade.remaining_quantity,
                    new_trigger_price=trade.sl_price,
                    trade_unit_id=trade.trade_unit_id,
                    tp_trigger_price=be_price,
                )

            trade.tp1_price = be_price
            trade.tp2_price = be_price
            trade.dd_be_triggered = True
            logger.info(
                "dd_be_triggered",
                trade_id=trade.id,
                symbol=trade.symbol,
                mark_price=mark_price,
                be_price=be_price,
                dd_pct=settings.dd_be_trigger_pct,
            )

    # ── Normal BE trigger ─────────────────────────────────────────────────────
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
    """BREAKEVEN → PARTIAL_EXIT: Close 50% of position in software when TP1 hit.

    No exchange TP1 order exists — the monitor closes the partial position
    directly via close_position when price reaches the TP1 trigger.
    """
    if not _check_trigger(mark_price, trade.entry_price, settings.trade_tp1_trigger_pct, trade.side):
        return StageResult()

    partial_size = trade.remaining_quantity if trade.dd_be_triggered else round(trade.quantity * settings.partial_exit_fraction, 8)

    # Size floor: ensure partial close meets minimum notional
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
        # Check actual position size to guard against race conditions
        position = exchange.get_position(trade.symbol)
        actual_size = abs(position.size) if position else 0.0
        expected_remaining = round(trade.quantity - partial_size, 8)

        if actual_size <= expected_remaining + 1e-9:
            logger.info(
                "tp1_already_filled_on_exchange",
                trade_id=trade.id,
                symbol=trade.symbol,
                actual_size=actual_size,
                expected_remaining=expected_remaining,
            )
        else:
            close_result = exchange.close_position(trade.symbol, trade.side, size=partial_size)
            if not close_result.success:
                logger.error("partial_close_failed", trade_id=trade.id, error=close_result.error)
                return StageResult(error=close_result.error)

    # Update trade state
    trade.remaining_quantity = round(trade.quantity - partial_size, 8)
    trade.partial_exit_done = True
    trade.tp1_fill_price = mark_price
    trade.tp1_order_id = None  # No exchange TP1 order

    # Update SL size to match remaining quantity
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
    """PARTIAL_EXIT → TRAILING_ACTIVE: Activate trailing stop when trail trigger hit."""
    if not _check_trigger(mark_price, trade.entry_price, settings.trade_trail_active_pct, trade.side):
        return StageResult()

    # No TP2 exchange order to cancel — trailing takes over in software
    trade.tp2_order_id = None

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
    """Ratchet trailing SL as price moves in our favour. SL never moves backward."""
    offset_pct = settings.trade_trail_lock_pct / 100

    if trade.side == TradeSide.LONG:
        if mark_price > trade.highest_price:
            old_watermark = trade.highest_price
            trade.highest_price = mark_price
            candidate_sl = trade.highest_price * (1 - offset_pct)
            if candidate_sl > trade.sl_price:
                if not _move_stop_loss(trade, candidate_sl, exchange, settings.sl_max_retries):
                    trade.highest_price = old_watermark
                else:
                    logger.debug("trailing_sl_ratcheted", trade_id=trade.id, new_sl=candidate_sl)
    else:
        if mark_price < trade.lowest_price:
            old_watermark = trade.lowest_price
            trade.lowest_price = mark_price
            candidate_sl = trade.lowest_price * (1 + offset_pct)
            if candidate_sl < trade.sl_price:
                if not _move_stop_loss(trade, candidate_sl, exchange, settings.sl_max_retries):
                    trade.lowest_price = old_watermark
                else:
                    logger.debug("trailing_sl_ratcheted", trade_id=trade.id, new_sl=candidate_sl)


def _check_tp2_exit(
    trade: Trade,
    mark_price: float,
    settings: Settings,
    exchange: LBankClient | None,
) -> StageResult | None:
    """Check if price hit TP2 hard exit. Closes remaining position in software."""
    if not _check_trigger(mark_price, trade.entry_price, settings.trade_tp2_trigger_pct, trade.side):
        return None

    if exchange and exchange.is_connected:
        position = exchange.get_position(trade.symbol)
        actual_size = abs(position.size) if position else 0.0
        if actual_size > 1e-9:
            exchange.close_position(trade.symbol, trade.side, size=trade.remaining_quantity)
        else:
            logger.info("tp2_already_filled_on_exchange", trade_id=trade.id, symbol=trade.symbol)

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

    Always returns transitioned=True to persist updated watermark/SL fields.
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

    Dispatches to the handler for the trade's current stage.
    Returns after at most ONE stage transition per call.
    The trade object is mutated in-place — caller persists changes.
    """
    handler = _STAGE_HANDLERS.get(trade.stage)
    if handler is None:
        return StageResult()
    return handler(trade, mark_price, settings, exchange)
