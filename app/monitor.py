"""Async price monitor loop for the trade lifecycle + signal evaluation.

Runs a single async loop that periodically:
1. Fetches all active trades from the database.
2. Fetches all prices in one batch call.
3. Evaluates each trade against its current mark price.
4. Persists any state changes.
5. Evaluates pending signals (every ~30s) via the Brain.

The loop runs every monitor_poll_interval_seconds (default 0.5s)
and can be stopped gracefully via request_stop().

Usage:
    monitor = TradeMonitor(settings, database, exchange_client)
    task = asyncio.create_task(monitor.run())
    # ... later ...
    monitor.request_stop()
    await task
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from app.brain import (
    calculate_exit_prices,
    calculate_trend_filters,
    check_pre_execution_filters,
    fetch_candles,
    fetch_2h_candles,
    get_counter_trend_sl,
    is_macro_aligned,
    is_slope_aligned,
    value_zone_multiplier,
)
from app.lifecycle import close_trade, evaluate_trade
from app.logging_setup import get_logger
from app.models import (
    AccountSnapshot,
    CloseReason,
    SignalStatus,
    Trade,
    TradeSide,
    TradeStage,
    MarketType,
)

if TYPE_CHECKING:
    from app.config import Settings
    from app.database import TradeDatabase
    from app.exchange import HyperliquidClient
    from app.models import Signal
    from app.telegram import TelegramNotifier

logger = get_logger("app.monitor")

# How often (in ticks) to run the position reconciliation check.
# At 0.5s poll interval, 10 ticks = every 5 seconds.
_RECONCILIATION_INTERVAL = 10

# How often (in ticks) to run signal evaluation.
# At 0.5s poll interval, 60 ticks = every 30 seconds.
_SIGNAL_EVAL_INTERVAL = 60


class TradeMonitor:
    """Async monitor loop that drives the trade lifecycle.

    Polls prices at a fixed interval and evaluates each active trade
    for stage transitions. Handles errors per-trade so one bad trade
    never kills the loop.
    """

    def __init__(
        self,
        settings: Settings,
        database: TradeDatabase,
        exchange: HyperliquidClient | None,
        notifier: TelegramNotifier | None = None,
    ) -> None:
        self._settings = settings
        self._database = database
        self._exchange = exchange
        self._notifier = notifier
        self._stop_event = asyncio.Event()
        self._tick_count: int = 0
        self._running: bool = False
        self._last_snapshot_slot: str | None = None

    @property
    def is_running(self) -> bool:
        """Whether the monitor loop is currently active."""
        return self._running

    def request_stop(self) -> None:
        """Signal the monitor loop to stop gracefully."""
        logger.info("monitor_stop_requested")
        self._stop_event.set()

    async def run(self) -> None:
        """Main monitor loop. Runs until stop_event is set.

        Uses asyncio.wait_for on the stop_event for responsive shutdown
        instead of asyncio.sleep (which wouldn't wake on stop).
        """
        self._running = True
        poll = self._settings.monitor_poll_interval_seconds

        # Pre-populate snapshot slot from DB so we don't re-fire on restart
        interval = self._settings.snapshot_interval_hours
        prev_snapshot = await self._database.get_latest_snapshot()
        if prev_snapshot:
            prev_ts = int(prev_snapshot.timestamp.timestamp())
            self._last_snapshot_slot = str(prev_ts // (interval * 3600))

        logger.info("monitor_started", poll_interval=poll)

        # Startup reconciliation — compare DB trades vs exchange positions
        try:
            await self._reconcile_on_startup()
        except Exception:
            logger.exception("startup_reconciliation_error")

        try:
            while not self._stop_event.is_set():
                try:
                    await self._tick()
                except Exception:
                    logger.exception("monitor_tick_error")

                self._tick_count += 1

                # Wait for either stop signal or poll interval
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=poll,
                    )
                    break  # stop_event was set
                except TimeoutError:
                    continue  # Normal: poll interval elapsed
        finally:
            self._running = False
            logger.info("monitor_stopped", total_ticks=self._tick_count)

    async def _tick(self) -> None:
        """Execute a single monitoring tick.

        Separated from run() for testability — tests can call
        _tick() directly without running the full async loop.
        """
        # Signal evaluation (every ~30s, not every tick)
        if self._tick_count > 0 and self._tick_count % _SIGNAL_EVAL_INTERVAL == 0:
            try:
                await self._evaluate_pending_signals()
            except Exception:
                logger.exception("signal_evaluation_error")

        # Memory signal evaluation (every ~60s)
        memory_interval = self._settings.memory_poll_interval_ticks
        if self._tick_count > 0 and self._tick_count % memory_interval == 0:
            try:
                await self._evaluate_memory_signals()
            except Exception:
                logger.exception("memory_evaluation_error")

        # Dynamic slope SL check (every ~60s)
        if self._settings.enable_slope_sl:
            slope_interval = self._settings.slope_sl_check_interval_ticks
            if self._tick_count > 0 and self._tick_count % slope_interval == 0:
                try:
                    await self._check_slope_stop_loss()
                except Exception:
                    logger.exception("slope_sl_check_error")

        # Account balance snapshot (lightweight check every tick)
        try:
            await self._check_snapshot_due()
        except Exception:
            logger.exception("snapshot_check_error")

        trades = await self._database.get_active_trades()
        if not trades:
            return

        # Batch price fetch — one API call for all symbols
        prices: dict[str, float] = {}
        if self._exchange and self._exchange.is_connected:
            prices = self._exchange.get_all_prices()

        if not prices:
            logger.debug("no_prices_available")
            return

        for trade in trades:
            mark_price = prices.get(trade.symbol)
            if mark_price is None:
                logger.warning(
                    "no_price_for_symbol",
                    symbol=trade.symbol,
                    trade_id=trade.id,
                )
                continue

            try:
                await self._evaluate_single_trade(trade, mark_price)
            except Exception:
                logger.exception(
                    "trade_evaluation_error",
                    trade_id=trade.id,
                    symbol=trade.symbol,
                )

    async def _evaluate_single_trade(
        self,
        trade: Trade,
        mark_price: float,
    ) -> None:
        """Evaluate one trade and persist changes if needed.

        Includes periodic position reconciliation to detect positions
        that were closed externally (SL/TP filled on exchange, manual close).
        """
        # Periodic reconciliation check
        if self._tick_count % _RECONCILIATION_INTERVAL == 0:
            position_exists = self._check_position_exists(trade)
            if not position_exists:
                await self._handle_position_gone(trade, mark_price=mark_price)
                return

        # Snapshot mutable fields before evaluation to detect changes
        _prev_sl = trade.sl_price
        _prev_high = trade.highest_price
        _prev_low = trade.lowest_price

        result = evaluate_trade(trade, mark_price, self._settings, self._exchange)

        if result.transitioned and result.new_stage is not None:
            success = await self._database.update_trade_stage(trade, result.new_stage)
            if not success:
                logger.error(
                    "stage_persist_failed",
                    trade_id=trade.id,
                    attempted_stage=result.new_stage.value,
                )
            else:
                await self._dispatch_stage_notification(trade, result.new_stage, mark_price)
        elif (
            trade.sl_price != _prev_sl
            or trade.highest_price != _prev_high
            or trade.lowest_price != _prev_low
        ):
            # No stage transition, but SL/watermarks changed (trailing ratchet).
            # Persist so restart recovery uses the latest values.
            await self._database.update_trade(trade)

        if result.error:
            logger.error(
                "stage_action_error",
                trade_id=trade.id,
                error=result.error,
            )
            # Alert on SL placement failures — trade may be unprotected
            if "SL" in result.error and self._notifier:
                await self._notifier.notify_sl_failed(trade, result.error)

    async def _dispatch_stage_notification(
        self,
        trade: Trade,
        new_stage: TradeStage,
        mark_price: float,
    ) -> None:
        """Dispatch the appropriate Telegram notification for a stage transition."""
        if not self._notifier:
            return

        if new_stage == TradeStage.BREAKEVEN:
            await self._notifier.notify_breakeven(trade, mark_price)

        elif new_stage == TradeStage.PARTIAL_EXIT:
            closed_size = round(trade.quantity - trade.remaining_quantity, 8)
            await self._notifier.notify_partial_close(trade, mark_price, closed_size)

        elif new_stage == TradeStage.TRAILING_ACTIVE:
            await self._notifier.notify_trailing_activated(trade, mark_price)

        elif new_stage == TradeStage.CLOSED:
            await self._notifier.notify_trade_closed(trade)

        # TRAILING_UPDATE: deliberately no notification (too noisy)

    def _check_position_exists(self, trade: Trade) -> bool:
        if not self._exchange or not self._exchange.is_connected:
            return True
        if trade.market_type == MarketType.SPOT:
            balance = self._exchange.get_spot_balance(trade.symbol)
            return balance > 0
        position = self._exchange.get_position(trade.symbol)
        return not (position is None or position.size == 0)

    async def _handle_position_gone(self, trade: Trade, mark_price: float | None = None) -> None:
        """Handle a position that no longer exists on exchange.

        Determines close reason based on the trade's current stage
        and marks it as CLOSED.

        Args:
            trade: The trade whose position is gone.
            mark_price: Current mark price for the symbol (for P&L calculation).
        """
        if trade.stage in (TradeStage.TRAILING_UPDATE, TradeStage.TRAILING_ACTIVE):
            reason = CloseReason.TRAILING_STOP
        elif trade.stage in (
            TradeStage.ENTRY,
            TradeStage.BREAKEVEN,
            TradeStage.PARTIAL_EXIT,
        ):
            reason = CloseReason.STOP_LOSS
        else:
            reason = CloseReason.MANUAL_CLOSE

        # Use SL price as close price for SL/trailing close when no mark_price available
        close_price = mark_price
        if close_price is None and reason in (CloseReason.STOP_LOSS, CloseReason.TRAILING_STOP):
            close_price = trade.sl_price

        # Cancel any remaining orders (e.g. TP2 left after SL fills)
        if self._exchange and self._exchange.is_connected:
            self._exchange.cancel_all_orders(trade.symbol)
            if trade.market_type == MarketType.PERP:
                self._exchange.close_position(trade.symbol)

        close_trade(trade, reason, close_price=close_price)
        await self._database.update_trade_stage(trade, TradeStage.CLOSED)

        logger.info(
            "trade_closed_position_gone",
            trade_id=trade.id,
            symbol=trade.symbol,
            reason=reason.value,
        )

        if self._notifier:
            await self._notifier.notify_trade_closed(trade)

    async def _reconcile_on_startup(self) -> None:
        """Compare DB active trades vs actual exchange positions on boot.
    
        Detects and handles:
        - DB trade exists but exchange position is gone (liquidated/closed while bot was down)
        - DB trade exists but exchange position size doesn't match (partial fill drift)
        - Exchange position exists with no matching DB trade (orphan — logged as warning)
        """
        if not self._exchange or not self._exchange.is_connected:
            logger.info("startup_reconciliation_skipped", reason="no exchange connection")
            return
    
        trades = await self._database.get_active_trades()
        if not trades:
            logger.info("startup_reconciliation_ok", active_trades=0)
            return
    
        prices = self._exchange.get_all_prices()
        tracked_symbols: set[str] = set()
    
        for trade in trades:
            tracked_symbols.add(trade.symbol)
    
            # --- SPOT reconciliation ---
            if trade.market_type == MarketType.SPOT:
                actual_size = self._exchange.get_spot_balance(trade.symbol)
                if actual_size <= 0:
                    mark_price = prices.get(trade.symbol)
                    logger.warning(
                        "startup_reconciliation_position_gone",
                        trade_id=trade.id,
                        symbol=trade.symbol,
                        stage=trade.stage.value,
                    )
                    await self._handle_position_gone(trade, mark_price=mark_price)
                    if self._notifier:
                        await self._notifier.send_message(
                            f"\u26a0\ufe0f <b>Startup Reconciliation</b>\n"
                            f"Trade {trade.symbol} ({trade.side.value.upper()}) "
                            f"spot balance is zero.\n"
                            f"Closed in DB as {trade.close_reason.value if trade.close_reason else 'unknown'}."
                        )
                    continue
    
                # Size check for spot
                expected_size = abs(trade.remaining_quantity)
                if expected_size > 0 and abs(actual_size - expected_size) / expected_size > 0.05:
                    logger.warning(
                        "startup_reconciliation_size_mismatch",
                        trade_id=trade.id,
                        symbol=trade.symbol,
                        db_remaining=expected_size,
                        exchange_size=actual_size,
                    )
                    if self._notifier:
                        await self._notifier.send_message(
                            f"\u26a0\ufe0f <b>Size Mismatch (Spot)</b>\n"
                            f"{trade.symbol}: DB expects {expected_size} "
                            f"but spot balance has {actual_size}"
                        )
                continue  # Skip perp SL checks for spot trades
    
            # --- PERP reconciliation ---
            position = self._exchange.get_position(trade.symbol)
    
            if position is None or position.size == 0:
                # Position gone — closed/liquidated while bot was down
                mark_price = prices.get(trade.symbol)
                logger.warning(
                    "startup_reconciliation_position_gone",
                    trade_id=trade.id,
                    symbol=trade.symbol,
                    stage=trade.stage.value,
                )
                await self._handle_position_gone(trade, mark_price=mark_price)
                if self._notifier:
                    await self._notifier.send_message(
                        f"\u26a0\ufe0f <b>Startup Reconciliation</b>\n"
                        f"Trade {trade.symbol} ({trade.side.value.upper()}) "
                        f"position no longer exists on exchange.\n"
                        f"Closed in DB as {trade.close_reason.value if trade.close_reason else 'unknown'}."
                    )
                continue
    
            # Position exists — check size alignment
            expected_size = abs(trade.remaining_quantity)
            actual_size = abs(position.size)
            if expected_size > 0 and abs(actual_size - expected_size) / expected_size > 0.05:
                logger.warning(
                    "startup_reconciliation_size_mismatch",
                    trade_id=trade.id,
                    symbol=trade.symbol,
                    db_remaining=expected_size,
                    exchange_size=actual_size,
                )
                if self._notifier:
                    await self._notifier.send_message(
                        f"\u26a0\ufe0f <b>Size Mismatch</b>\n"
                        f"{trade.symbol}: DB expects {expected_size} "
                        f"but exchange has {actual_size}"
                    )
    
            # Check SL orders on exchange — distinguish SL from TP by price direction
            # SL for longs: price BELOW entry
            # SL for shorts: price ABOVE entry
            try:
                open_orders = self._exchange.info.open_orders(self._exchange.wallet_address)
                is_long = trade.side == TradeSide.LONG
                entry_price = trade.entry_price
    
                symbol_sl_orders = [
                    o for o in open_orders
                    if o["coin"] == trade.symbol
                    and o.get("reduceOnly", False)
                    and (
                        (is_long and float(o["limitPx"]) < entry_price) or
                        (not is_long and float(o["limitPx"]) > entry_price)
                    )
                ]
    
                logger.info(
                    "startup_reconciliation_sl_check",
                    trade_id=trade.id,
                    symbol=trade.symbol,
                    side=trade.side.value,
                    entry_price=entry_price,
                    sl_orders_found=len(symbol_sl_orders),
                )
    
                if len(symbol_sl_orders) == 0:
                    # No SL at all — place one at the DB's latest sl_price
                    logger.warning(
                        "startup_reconciliation_missing_sl",
                        trade_id=trade.id,
                        symbol=trade.symbol,
                        sl_price=trade.sl_price,
                    )
                    sl_result = self._exchange.place_stop_loss(
                        trade.symbol,
                        trade.side,
                        actual_size,
                        trade.sl_price,
                    )
                    if sl_result.success:
                        trade.sl_order_id = sl_result.order_id
                        await self._database.update_trade(trade)
                        logger.info(
                            "startup_reconciliation_sl_placed",
                            trade_id=trade.id,
                            symbol=trade.symbol,
                            sl_price=trade.sl_price,
                        )
                    else:
                        logger.error(
                            "startup_reconciliation_sl_failed",
                            trade_id=trade.id,
                            error=sl_result.error,
                        )
                    if self._notifier:
                        status = "placed" if sl_result.success else "FAILED"
                        await self._notifier.send_message(
                            f"\u26a0\ufe0f <b>Missing SL Recovered</b>\n"
                            f"{trade.symbol}: SL at {trade.sl_price:.2f} — {status}"
                        )
                elif len(symbol_sl_orders) > 1:
                    # Multiple SLs — cancel all but the tightest one
                    # For longs: tightest = highest price (closest to entry from below)
                    # For shorts: tightest = lowest price (closest to entry from above)
                    symbol_sl_orders.sort(
                        key=lambda o: float(o["limitPx"]),
                        reverse=is_long,
                    )
                    keep = symbol_sl_orders[0]
                    stale = symbol_sl_orders[1:]
                    for o in stale:
                        try:
                            self._exchange.exchange.cancel(trade.symbol, o["oid"])
                            logger.info(
                                "startup_reconciliation_cancelled_stale_sl",
                                trade_id=trade.id,
                                cancelled_oid=o["oid"],
                                cancelled_price=o["limitPx"],
                            )
                        except Exception:
                            logger.exception(
                                "startup_reconciliation_cancel_failed",
                                oid=o["oid"],
                            )
                    trade.sl_order_id = str(keep["oid"])
                    await self._database.update_trade(trade)
                    if self._notifier:
                        await self._notifier.send_message(
                            f"\u26a0\ufe0f <b>Duplicate SLs Cleaned</b>\n"
                            f"{trade.symbol}: Kept SL at {keep['limitPx']}, "
                            f"cancelled {len(stale)} stale order(s)"
                        )
                else:
                    # Exactly 1 SL — make sure DB tracks the right order ID
                    trade.sl_order_id = str(symbol_sl_orders[0]["oid"])
                    await self._database.update_trade(trade)
            except Exception:
                logger.exception("startup_reconciliation_sl_check_error", trade_id=trade.id)
    
        logger.info(
            "startup_reconciliation_complete",
            trades_checked=len(trades),
            symbols=list(tracked_symbols),
        )
        
    async def _close_trade_for_flip(self, trade: Trade) -> None:
        """Close an existing trade to flip direction.

        Called when an opposing signal passes all filters and is about
        to enter. Closes the current position on exchange, cancels all
        orders, and marks the trade as CLOSED with DIRECTION_FLIP reason.
        """
        close_price: float | None = None
        if self._exchange and self._exchange.is_connected:
            close_price = self._exchange.get_mark_price(trade.symbol)
            self._exchange.cancel_all_orders(trade.symbol)
            
            if trade.market_type == MarketType.SPOT:
                self._exchange.close_spot_position(trade.symbol, trade.remaining_quantity)
            else:
                self._exchange.close_position(trade.symbol)

        close_trade(trade, CloseReason.DIRECTION_FLIP, close_price=close_price)
        await self._database.update_trade_stage(trade, TradeStage.CLOSED)

        logger.info(
            "trade_closed_direction_flip",
            trade_id=trade.id,
            symbol=trade.symbol,
            side=trade.side.value,
            close_price=close_price,
        )

        if self._notifier:
            await self._notifier.notify_trade_closed(trade)

    # --- Signal Evaluation (Phase 6: The Brain) ---

    async def _evaluate_pending_signals(self) -> None:
        """Evaluate all pending signals that have matured past the delay.

        Runs every ~30s. For each mature signal:
        1. Check if already in position for this symbol
        2. Fetch 15m candles via CCXT
        3. Calculate trend filters
        4. Check trend alignment
        5. Apply value zone sizing
        6. Execute entry if approved
        """
        signals = await self._database.get_pending_signals()
        if not signals:
            return

        delay = timedelta(minutes=self._settings.signal_eval_delay_minutes)
        now = datetime.now(UTC)

        for sig in signals:
            age = now - sig.created_at
            if age < delay:
                continue  # Not mature yet

            try:
                await self._evaluate_single_signal(sig)
            except Exception:
                logger.exception(
                    "single_signal_eval_error",
                    signal_id=sig.id,
                    symbol=sig.symbol,
                )
                sig.status = SignalStatus.ERROR
                sig.rejection_reason = "Evaluation error"
                sig.evaluated_at = datetime.now(UTC)
                await self._database.update_signal(sig)

    async def _evaluate_single_signal(self, sig: Signal) -> None:
        """Evaluate a single pending signal through the Brain pipeline.

        Args:
            sig: The signal to evaluate.
        """
        logger.info(
            "evaluating_signal",
            signal_id=sig.id,
            symbol=sig.symbol,
            action=sig.action.value,
        )

        # 1. Check if already in position for this symbol
        existing = await self._database.get_trades_by_symbol(sig.symbol)
        if existing:
            signal_side = TradeSide.LONG if "long" in sig.action.value else TradeSide.SHORT
            same_direction = [t for t in existing if t.side == signal_side]
            if same_direction:
                logger.info(
                    "signal_rejected_position",
                    signal_id=sig.id,
                    symbol=sig.symbol,
                    existing_trade=same_direction[0].id,
                )
                sig.status = SignalStatus.REJECTED_POSITION
                sig.rejection_reason = f"Already in same-direction position: {same_direction[0].id}"
                sig.evaluated_at = datetime.now(UTC)
                await self._database.update_signal(sig)
                return
            # Opposing trade exists — will be handled at execution time, not during memory halt
            logger.info(
                "opposing_position_detected_deferred",
                signal_id=sig.id,
                symbol=sig.symbol,
                existing_side=existing[0].side.value,
            )

        # 1b. Cooldown check — block re-entry after Slope SL close
        if await self._is_in_cooldown(sig.symbol):
            logger.info(
                "signal_rejected_cooldown",
                signal_id=sig.id,
                symbol=sig.symbol,
                cooldown_minutes=self._settings.signal_cooldown_minutes,
            )
            sig.status = SignalStatus.REJECTED_FILTERS
            sig.rejection_reason = (
                f"Cooldown: slope SL triggered within last "
                f"{self._settings.signal_cooldown_minutes} min"
            )
            sig.evaluated_at = datetime.now(UTC)
            await self._database.update_signal(sig)
            return

        # 2. Fetch candles at configured timeframe
        df = await fetch_candles(
            sig.symbol,
            self._settings.ccxt_exchange_source,
            limit=self._settings.candle_fetch_limit,
            timeframe=self._settings.candle_timeframe,
        )

        # 2b. Fetch separate ATR candles if ATR regime uses a different timeframe
        atr_df = None
        if self._settings.use_atr_regime and self._settings.atr_timeframe != self._settings.candle_timeframe:
            atr_df = await fetch_candles(
                sig.symbol,
                self._settings.ccxt_exchange_source,
                limit=self._settings.atr_candle_limit,
                timeframe=self._settings.atr_timeframe,
            )

        # 3. Calculate trend filters
        trend = calculate_trend_filters(
            df,
            ema_macro_span=self._settings.ema_macro_span,
            slope_smooth_bars=self._settings.slope_smooth_bars,
            delta_smooth_bars=self._settings.delta_smooth_bars,
            scale_window=int(self._settings.ema_calibration_base * self._settings.ema_calibration_factor),
            slope_method=self._settings.slope_method.value,
            use_delta_ema=self._settings.use_delta_ema,
            delta_ntz=self._settings.delta_ntz,
            use_atr_regime=self._settings.use_atr_regime,
            atr_length=self._settings.atr_length,
            atr_ema_length=self._settings.atr_ema_length,
            atr_fast_threshold=self._settings.atr_fast_threshold,
            atr_df=atr_df,
        )

        # Log analytics to signal (T and T-1 values + last 5 history)
        sig.ema_slope_value = trend.ema_scaled
        sig.ema_slope_prev = trend.ema_scaled_prev
        sig.delta_slope_value = trend.delta
        sig.delta_slope_prev = trend.delta_prev
        sig.slope_rising = trend.slope_rising
        sig.ema_slope_history = trend.ema_slope_history
        sig.delta_slope_history = trend.delta_slope_history
        sig.atr_regime_pct = trend.atr_regime_pct
        sig.is_fast_market = trend.is_fast_market

        logger.info(
            "trend_data",
            signal_id=sig.id,
            symbol=sig.symbol,
            action=sig.action.value,
            ema_scaled=round(trend.ema_scaled, 4),
            delta=round(trend.delta, 4),
            slope_rising=trend.slope_rising,
        )

        # 4a. HARD GATE — slope must be hooking in trade direction
        # Always sends to memory halt if slope not aligned (counter-trend waits too)
        is_counter_trend = False
        if not is_slope_aligned(sig.action.value, trend):
            logger.info(
                "signal_entering_memory_slope",
                signal_id=sig.id,
                symbol=sig.symbol,
                ema_scaled=round(trend.ema_scaled, 4),
                slope_rising=trend.slope_rising,
                counter_trend_enabled=self._settings.allow_counter_trend_half_size,
            )
            sig.status = SignalStatus.PENDING_MEMORY
            sig.memory_entered_at = datetime.now(UTC)
            sig.last_memory_slope = trend.ema_scaled
            sig.rejection_reason = (
                f"Slope not aligned: slope_rising={trend.slope_rising}"
            )
            await self._database.update_signal(sig)
            if self._notifier:
                await self._notifier.notify_signal_memory(sig)
            return

        # 4b. SOFT GATE — macro trend (ema_scaled > 0 for longs, < 0 for shorts)
        # If slope is aligned but macro isn't, allow as counter-trend at half size
        if not is_macro_aligned(sig.action.value, trend):
            if self._settings.allow_counter_trend_half_size:
                # Slope aligned but macro against — counter-trend at half size
                is_counter_trend = True
                logger.info(
                    "counter_trend_half_size",
                    signal_id=sig.id,
                    symbol=sig.symbol,
                    ema_scaled=round(trend.ema_scaled, 4),
                    slope_rising=trend.slope_rising,
                    original_size=sig.size_usd,
                    halved_size=sig.size_usd * 0.5,
                )
                # Don't mutate sig.size_usd — halving applied at actual_size calculation
            else:
                # Strict: memory halt
                logger.info(
                    "signal_entering_memory_macro",
                    signal_id=sig.id,
                    symbol=sig.symbol,
                    ema_scaled=round(trend.ema_scaled, 4),
                    slope_rising=trend.slope_rising,
                )
                sig.status = SignalStatus.PENDING_MEMORY
                sig.memory_entered_at = datetime.now(UTC)
                sig.last_memory_slope = trend.ema_scaled
                sig.rejection_reason = (
                    f"Macro not aligned: ema_scaled={trend.ema_scaled:.4f}"
                )
                await self._database.update_signal(sig)
                if self._notifier:
                    await self._notifier.notify_signal_memory(sig)
                return

        # 5. Get current price
        current_price: float | None = None
        if self._exchange and self._exchange.is_connected:
            current_price = self._exchange.get_mark_price(sig.symbol)

        if current_price is None:
            current_price = sig.entry_price  # Fallback

        sig.eval_price = current_price
        is_long = "long" in sig.action.value

        df_2h = await fetch_2h_candles(sig.symbol, self._settings.ccxt_exchange_source) 
        
        # 5b. Pre-execution filters (Phase 2)
        filter_result = check_pre_execution_filters(
            trend,
            current_price,
            self._settings,
            is_long=is_long,
            is_counter_trend=is_counter_trend,  
            df_2h=df_2h,                       
        )
        if not filter_result.passed:
            if filter_result.should_wait:
                # Overextended or chop — leave signal as-is, retry next cycle
                logger.info(
                    "signal_filter_wait",
                    signal_id=sig.id,
                    reason=filter_result.rejection_reason,
                )
                return
            # Hard reject (e.g. chase with REJECT action)
            sig.status = SignalStatus.REJECTED_FILTERS
            sig.rejection_reason = filter_result.rejection_reason
            sig.evaluated_at = datetime.now(UTC)
            await self._database.update_signal(sig)
            if self._notifier:
                await self._notifier.notify_signal_rejected(sig)
            return

        # 6. Apply sizing — any half-size condition caps at 0.5×, never stacks
        vz_multiplier = value_zone_multiplier(df, is_long, self._settings.vz_memory_bars)

        if is_counter_trend:
            actual_size = sig.size_usd * 0.5  # Hard cap, nothing else applies
        elif vz_multiplier < 1.0 or filter_result.size_multiplier < 1.0:
            actual_size = sig.size_usd * 0.5  # Any single condition → 0.5x
        else:
            actual_size = sig.size_usd  # Full size

        sig.actual_size_usd = actual_size

        # Determine half-size reason for notifications
        if is_counter_trend:
            half_size_reason = "counter-trend"
        elif vz_multiplier < 1.0:
            half_size_reason = "outside value zone"
        elif filter_result.size_multiplier < 1.0:
            half_size_reason = "anti-chase filter"
        else:
            half_size_reason = ""

        ## 6b. Detect and close opposing trade at execution time (not during memory halt)
        is_opposing_flip = False
        opposing_trade = None
        existing_at_execution = await self._database.get_trades_by_symbol(sig.symbol)

        if existing_at_execution:
            signal_side = TradeSide.LONG if "long" in sig.action.value else TradeSide.SHORT
            opposite = [t for t in existing_at_execution if t.side != signal_side]
            if opposite:
                is_opposing_flip = True
                opposing_trade = opposite[0]
        
        if is_opposing_flip and opposing_trade is not None:
            await self._close_trade_for_flip(opposing_trade)
            await asyncio.sleep(1)

        fresh_sig = await self._database.get_signal(sig.id)
        if fresh_sig is None or fresh_sig.status != SignalStatus.PENDING_EVAL:
            logger.info(
                "signal_already_processed",
                signal_id=sig.id,
                status=fresh_sig.status.value if fresh_sig else "not_found",
            )
            return
        
        # 7. Execute entry
        await self._execute_signal_entry(sig, actual_size, is_long, is_counter_trend=is_counter_trend, half_size_reason=half_size_reason)

    async def _execute_signal_entry(
        self,
        sig: Signal,
        size_usd: float,
        is_long: bool,
        *,
        is_counter_trend: bool = False,
        half_size_reason: str = "",
    ) -> None:
        """Place orders and create a Trade from an approved signal.
    
        Args:
            sig: The approved signal.
            size_usd: Actual trade size after value zone adjustment.
            is_long: True for long, False for short.
            is_counter_trend: True if this is a counter-trend trade (uses structural SL).
            half_size_reason: Why trade was half-sized (empty if full size).
        """
        symbol = sig.symbol
        side = TradeSide.LONG if is_long else TradeSide.SHORT
        is_spot = sig.market_type == MarketType.SPOT
    
        # Calculate exit prices from settings
        exits = calculate_exit_prices(
            sig.eval_price or sig.entry_price,
            is_long,
            self._settings,
        )
    
        quantity: float | None = None
        order_id: str | None = None
        fill_price = sig.eval_price or sig.entry_price
        sl_order_id: str | None = None
        tp1_order_id: str | None = None
        tp2_order_id: str | None = None
    
        if self._exchange and self._exchange.is_connected:
            # Calculate size in contracts
            quantity = self._exchange.calculate_order_size(symbol, size_usd)
            if quantity is None:
                sig.status = SignalStatus.ERROR
                sig.rejection_reason = "Could not calculate order size"
                sig.evaluated_at = datetime.now(UTC)
                await self._database.update_signal(sig)
                return
    
            # Place market order — route by market type
            if is_spot:
                result = self._exchange.place_spot_market_order(symbol, True, quantity)
            else:
                result = self._exchange.place_market_order(symbol, side, quantity)
    
            if not result.success:
                sig.status = SignalStatus.ERROR
                sig.rejection_reason = f"Order failed: {result.error}"
                sig.evaluated_at = datetime.now(UTC)
                await self._database.update_signal(sig)
                return
    
            order_id = result.order_id
            if result.avg_price:
                fill_price = result.avg_price
                # Recalculate exits with actual fill price
                exits = calculate_exit_prices(fill_price, is_long, self._settings)
    
            # Partial fill detection — use actual filled size for SL/TP placement
            if isinstance(result.filled_size, (int, float)) and result.filled_size > 0:
                if abs(result.filled_size - quantity) / quantity > 0.01:
                    logger.warning(
                        "partial_fill_detected",
                        signal_id=sig.id,
                        symbol=symbol,
                        intended_size=quantity,
                        filled_size=result.filled_size,
                    )
                    if self._notifier:
                        await self._notifier.send_message(
                            f"\u26a0\ufe0f <b>Partial Fill</b>\n"
                            f"{symbol}: Intended {quantity}, filled {result.filled_size}"
                        )
                quantity = result.filled_size  # Use actual fill for all subsequent orders
    
            # Structural SL from 2H candles for all trades.
            try:
                df_2h = await fetch_2h_candles(
                    sig.symbol,
                    self._settings.ccxt_exchange_source,
                )
                structural_sl = get_counter_trend_sl(df_2h, fill_price, is_long, self._settings.sl_buffer_pct)
                if structural_sl is not None:
                    pct_sl = exits.sl_price
                    if is_long:
                        final_sl = max(structural_sl, pct_sl)
                    else:
                        final_sl = min(structural_sl, pct_sl)
                    logger.info(
                        "structural_sl_override",
                        symbol=sig.symbol,
                        percentage_sl=pct_sl,
                        structural_sl=structural_sl,
                        final_sl=final_sl,
                        winner="structural" if final_sl == structural_sl else "percentage_cap",
                        source="2h_low" if is_long else "2h_high",
                        is_counter_trend=is_counter_trend,
                    )
                    exits.sl_price = final_sl
                else:
                    logger.warning(
                        "structural_sl_fallback",
                        symbol=sig.symbol,
                        reason="2H level invalid or beyond entry, using percentage SL",
                        fallback_sl=exits.sl_price,
                    )
            except Exception as e:
                logger.warning(
                    "structural_sl_fetch_failed",
                    symbol=sig.symbol,
                    error=str(e),
                    fallback_sl=exits.sl_price,
                )
    
            tp1_size = round(quantity * self._settings.partial_exit_fraction, 8)
            tp2_size = round(quantity - tp1_size, 8)
    
            # Place SL — route by market type, close position if it fails
            if is_spot:
                sl_result = self._exchange.place_spot_stop_loss(symbol, quantity, exits.sl_price)
            else:
                sl_result = self._exchange.place_stop_loss(symbol, side, quantity, exits.sl_price)
    
            if not sl_result.success:
                logger.error(
                    "sl_placement_failed_closing_position",
                    symbol=symbol,
                    sl_price=exits.sl_price,
                    error=sl_result.error,
                )
                self._exchange.cancel_all_orders(symbol)
                if is_spot:
                    self._exchange.close_spot_position(symbol, quantity)
                else:
                    self._exchange.close_position(symbol)
                sig.status = SignalStatus.REJECTED_FILTERS
                sig.rejection_reason = f"SL order rejected: {sl_result.error}"
                sig.evaluated_at = datetime.now(UTC)
                await self._database.update_signal(sig)
                if self._notifier:
                    await self._notifier.notify_signal_rejected(sig)
                return
            sl_order_id = sl_result.order_id
    
            # Place TP1 + TP2 — route by market type
            if is_spot:
                tp1_result = self._exchange.place_spot_take_profit(symbol, tp1_size, exits.tp1_price)
                tp2_result = self._exchange.place_spot_take_profit(symbol, tp2_size, exits.tp2_price)
            else:
                tp1_result = self._exchange.place_take_profit(symbol, side, tp1_size, exits.tp1_price)
                tp2_result = self._exchange.place_take_profit(symbol, side, tp2_size, exits.tp2_price)
    
            tp1_order_id = tp1_result.order_id if tp1_result.success else None
            tp2_order_id = tp2_result.order_id if tp2_result.success else None
        else:
            # Paper mode
            quantity = size_usd / fill_price if fill_price > 0 else 0.001
            logger.warning("signal_entry_paper_mode", symbol=symbol)
    
        # Create trade record
        trade = Trade(
            symbol=symbol,
            side=side,
            stage=TradeStage.ENTRY,
            entry_price=fill_price,
            sl_price=exits.sl_price,
            tp1_price=exits.tp1_price,
            tp2_price=exits.tp2_price,
            quantity=quantity,
            entry_order_id=order_id,
            sl_order_id=sl_order_id,
            tp1_order_id=tp1_order_id,
            tp2_order_id=tp2_order_id,
            signal_id=sig.id,
            ema_slope_value=sig.ema_slope_value,
            delta_slope_value=sig.delta_slope_value,
            slope_rising=sig.slope_rising,
            atr_regime_pct=sig.atr_regime_pct,
            is_fast_market=sig.is_fast_market,
            market_type=sig.market_type,  # ADD
        )
    
        await self._database.save_trade(trade)
    
        # Update signal as approved
        sig.status = SignalStatus.APPROVED
        sig.trade_id = trade.id
        sig.evaluated_at = datetime.now(UTC)
        await self._database.update_signal(sig)
    
        logger.info(
            "trade_audit_entry",
            signal_id=sig.id,
            trade_id=trade.id,
            symbol=symbol,
            side=side.value,
            market_type=sig.market_type.value,
            fill_price=fill_price,
            quantity=quantity,
            size_usd=size_usd,
            sl_price=exits.sl_price,
            tp1_price=exits.tp1_price,
            tp2_price=exits.tp2_price,
            entry_order_id=order_id,
            sl_order_id=sl_order_id,
            tp1_order_id=tp1_order_id,
            tp2_order_id=tp2_order_id,
            is_counter_trend=is_counter_trend,
            half_size_reason=half_size_reason,
        )
    
        if self._notifier:
            await self._notifier.notify_trade_opened(trade, sig, is_counter_trend=is_counter_trend, half_size_reason=half_size_reason)

    # --- Memory Halt Evaluation (Phase 2) ---

    async def _evaluate_memory_signals(self) -> None:
        """Evaluate all signals in PENDING_MEMORY status.

        For each memory signal, check if slope has recovered enough
        to re-attempt entry, or if macro structure has broken.
        """
        signals = await self._database.get_memory_signals()
        if not signals:
            return

        for sig in signals:
            try:
                await self._evaluate_single_memory_signal(sig)
            except Exception:
                logger.exception(
                    "memory_signal_eval_error",
                    signal_id=sig.id,
                    symbol=sig.symbol,
                )

    async def _evaluate_single_memory_signal(self, sig: Signal) -> None:
        """Evaluate a single PENDING_MEMORY signal for recovery.

        Steps:
        1. Check existing position → REJECTED_POSITION
        2. Fetch candles + trend
        3. Macro check: 50/200 EMA structure intact
        4. Slope recovery check
        5. If recovering: run pre-execution filters → execute
        """
        
        # 1. Check existing position
        existing = await self._database.get_trades_by_symbol(sig.symbol)
        if existing:
            signal_side = TradeSide.LONG if "long" in sig.action.value else TradeSide.SHORT
            same_direction = [t for t in existing if t.side == signal_side]
            if same_direction:
                sig.status = SignalStatus.REJECTED_POSITION
                sig.rejection_reason = f"Position opened while in memory (same direction): {same_direction[0].id}"
                sig.evaluated_at = datetime.now(UTC)
                await self._database.update_signal(sig)
                return
            # Opposing trade exists — will be handled at execution time, not during memory halt
            logger.info(
                "opposing_position_detected_deferred",
                signal_id=sig.id,
                symbol=sig.symbol,
                existing_side=existing[0].side.value,
            )
        
        # 2. Fetch candles + trend
        df = await fetch_candles(
            sig.symbol,
            self._settings.ccxt_exchange_source,
            limit=self._settings.candle_fetch_limit,
            timeframe=self._settings.candle_timeframe,
        )
        # Fetch separate ATR candles if ATR regime uses a different timeframe
        atr_df = None
        if self._settings.use_atr_regime and self._settings.atr_timeframe != self._settings.candle_timeframe:
            atr_df = await fetch_candles(
                sig.symbol,
                self._settings.ccxt_exchange_source,
                limit=self._settings.atr_candle_limit,
                timeframe=self._settings.atr_timeframe,
            )
        trend = calculate_trend_filters(
            df,
            ema_macro_span=self._settings.ema_macro_span,
            slope_smooth_bars=self._settings.slope_smooth_bars,
            delta_smooth_bars=self._settings.delta_smooth_bars,
            scale_window=int(self._settings.ema_calibration_base * self._settings.ema_calibration_factor),
            slope_method=self._settings.slope_method.value,
            use_delta_ema=self._settings.use_delta_ema,
            delta_ntz=self._settings.delta_ntz,
            use_atr_regime=self._settings.use_atr_regime,
            atr_length=self._settings.atr_length,
            atr_ema_length=self._settings.atr_ema_length,
            atr_fast_threshold=self._settings.atr_fast_threshold,
            atr_df=atr_df,
        )

        is_long = "long" in sig.action.value

        # 3. Macro check: 50/200 EMA structure
        # When counter-trend is ON, skip this kill — let the signal keep waiting
        # for slope to recover, then enter at half size against the macro trend.
        if not self._settings.allow_counter_trend_half_size:
            if is_long and trend.ema_50 < trend.ema_200:
                sig.status = SignalStatus.EXPIRED_MACRO_BROKEN
                sig.rejection_reason = "Macro broken: EMA-50 < EMA-200 (long)"
                sig.evaluated_at = datetime.now(UTC)
                await self._database.update_signal(sig)
                if self._notifier:
                    await self._notifier.notify_signal_rejected(sig)
                return

            if not is_long and trend.ema_50 > trend.ema_200:
                sig.status = SignalStatus.EXPIRED_MACRO_BROKEN
                sig.rejection_reason = "Macro broken: EMA-50 > EMA-200 (short)"
                sig.evaluated_at = datetime.now(UTC)
                await self._database.update_signal(sig)
                if self._notifier:
                    await self._notifier.notify_signal_rejected(sig)
                return

        # 4. Slope recovery check
        current_slope = trend.ema_scaled
        prev_slope = sig.last_memory_slope or 0.0

        recovering = (is_long and current_slope >= prev_slope) or (
            not is_long and current_slope <= prev_slope
        )

        if not recovering:
            # Stay in memory — update tracking fields
            sig.last_memory_slope = current_slope
            sig.memory_eval_count += 1
            await self._database.update_signal(sig)
            logger.info(
                "memory_signal_not_recovering",
                signal_id=sig.id,
                symbol=sig.symbol,
                current_slope=round(current_slope, 4),
                prev_slope=round(prev_slope, 4),
                eval_count=sig.memory_eval_count,
            )
            return

        # 5. Slope is recovering — check alignment gates
        is_counter_trend = False
        if not is_slope_aligned(sig.action.value, trend):
            # Slope still not hooking in trade direction, stay in memory
            sig.last_memory_slope = current_slope
            sig.memory_eval_count += 1
            await self._database.update_signal(sig)
            return

        # Slope is now aligned — check macro alignment
        if not is_macro_aligned(sig.action.value, trend):
            if self._settings.allow_counter_trend_half_size:
                # Slope aligned but macro against — counter-trend at half size
                is_counter_trend = True
            else:
                # Macro not aligned and counter-trend disabled, stay in memory
                sig.last_memory_slope = current_slope
                sig.memory_eval_count += 1
                await self._database.update_signal(sig)
                return

        # 6. Run pre-execution filters
        current_price: float | None = None
        if self._exchange and self._exchange.is_connected:
            current_price = self._exchange.get_mark_price(sig.symbol)
        if current_price is None:
            current_price = sig.entry_price

        sig.eval_price = current_price
        sig.ema_slope_value = trend.ema_scaled
        sig.ema_slope_prev = trend.ema_scaled_prev
        sig.delta_slope_value = trend.delta
        sig.delta_slope_prev = trend.delta_prev
        sig.slope_rising = trend.slope_rising
        sig.ema_slope_history = trend.ema_slope_history
        sig.delta_slope_history = trend.delta_slope_history

        df_2h = await fetch_2h_candles(sig.symbol, self._settings.ccxt_exchange_source)
        filter_result = check_pre_execution_filters(
            trend,
            current_price,
            self._settings,
            is_long=is_long,
            is_counter_trend=is_counter_trend,
            df_2h=df_2h,
        )


        if not filter_result.passed:
            if filter_result.should_wait:
                sig.last_memory_slope = current_slope
                sig.memory_eval_count += 1
                await self._database.update_signal(sig)
                return
            # Hard reject
            sig.status = SignalStatus.REJECTED_FILTERS
            sig.rejection_reason = filter_result.rejection_reason
            sig.evaluated_at = datetime.now(UTC)
            await self._database.update_signal(sig)
            if self._notifier:
                await self._notifier.notify_signal_rejected(sig)
            return

        # 7. All checks passed — any half-size condition caps at 0.5×, never stacks
        vz_multiplier = value_zone_multiplier(df, is_long, self._settings.vz_memory_bars)
        
        if is_counter_trend:
            actual_size = sig.size_usd * 0.5
        elif vz_multiplier < 1.0 or filter_result.size_multiplier < 1.0:
            actual_size = sig.size_usd * 0.5
        else:
            actual_size = sig.size_usd
        
        sig.actual_size_usd = actual_size

        # Determine half-size reason for notifications
        if is_counter_trend:
            half_size_reason = "counter-trend"
        elif vz_multiplier < 1.0:
            half_size_reason = "outside value zone"
        elif filter_result.size_multiplier < 1.0:
            half_size_reason = "anti-chase filter"
        else:
            half_size_reason = ""

        logger.info(
            "memory_signal_recovered",
            signal_id=sig.id,
            symbol=sig.symbol,
            slope=round(current_slope, 4),
            eval_count=sig.memory_eval_count,
            is_counter_trend=is_counter_trend,
            half_size_reason=half_size_reason,
            original_size=sig.size_usd,
            actual_size=actual_size,
        )

        # Close opposing trade before entering new direction
        is_opposing_flip = False
        opposing_trade = None
        existing_at_execution = await self._database.get_trades_by_symbol(sig.symbol)
        if existing_at_execution:
            signal_side = TradeSide.LONG if "long" in sig.action.value else TradeSide.SHORT
            opposite = [t for t in existing_at_execution if t.side != signal_side]
            if opposite:
                is_opposing_flip = True
                opposing_trade = opposite[0]
        
        if is_opposing_flip and opposing_trade is not None:
            await self._close_trade_for_flip(opposing_trade)
            await asyncio.sleep(1)
            
        await self._execute_signal_entry(sig, actual_size, is_long, is_counter_trend=is_counter_trend, half_size_reason=half_size_reason)

    # --- Cooldown Guardrail (Phase 2) ---

    async def _is_in_cooldown(self, symbol: str) -> bool:
        """Check if a symbol is blocked after a recent Slope SL close."""
        last_trade = await self._database.get_last_closed_trade(symbol)
        if (
            last_trade
            and last_trade.close_reason == CloseReason.SLOPE_REVERSAL
            and last_trade.closed_at
        ):
            elapsed = datetime.now(UTC) - last_trade.closed_at
            if elapsed < timedelta(minutes=self._settings.signal_cooldown_minutes):
                return True
        return False

    # --- Dynamic Slope Stop-Loss (Phase 2) ---

    async def _check_slope_stop_loss(self) -> None:
        """Check EMA50/EMA200 cross for all active trend-following trades.
    
        Closes a trade when the macro trend structure flips:
        - LONG: close if EMA-50 crosses below EMA-200 (death cross)
        - SHORT: close if EMA-50 crosses above EMA-200 (golden cross)
    
        Counter-trend trades are skipped — they already trade against
        the macro trend so a cross doesn't signal an exit.
        """
        trades = await self._database.get_active_trades()
        if not trades:
            return
    
        # Group by symbol to avoid duplicate candle fetches
        by_symbol: dict[str, list[Trade]] = {}
        for trade in trades:
            by_symbol.setdefault(trade.symbol, []).append(trade)
    
        for symbol, symbol_trades in by_symbol.items():
            try:
                df = await fetch_candles(
                    symbol,
                    self._settings.ccxt_exchange_source,
                    limit=self._settings.candle_fetch_limit,
                    timeframe=self._settings.candle_timeframe,
                )
                atr_df = None
                if self._settings.use_atr_regime and self._settings.atr_timeframe != self._settings.candle_timeframe:
                    atr_df = await fetch_candles(
                        symbol,
                        self._settings.ccxt_exchange_source,
                        limit=self._settings.atr_candle_limit,
                        timeframe=self._settings.atr_timeframe,
                    )
                trend = calculate_trend_filters(
                    df,
                    ema_macro_span=self._settings.ema_macro_span,
                    slope_smooth_bars=self._settings.slope_smooth_bars,
                    delta_smooth_bars=self._settings.delta_smooth_bars,
                    scale_window=int(self._settings.ema_calibration_base * self._settings.ema_calibration_factor),
                    slope_method=self._settings.slope_method.value,
                    use_delta_ema=self._settings.use_delta_ema,
                    delta_ntz=self._settings.delta_ntz,
                    use_atr_regime=self._settings.use_atr_regime,
                    atr_length=self._settings.atr_length,
                    atr_ema_length=self._settings.atr_ema_length,
                    atr_fast_threshold=self._settings.atr_fast_threshold,
                    atr_df=atr_df,
                )
            except Exception:
                logger.exception("slope_sl_candle_fetch_error", symbol=symbol)
                continue
    
            for trade in symbol_trades:
                # Skip counter-trend trades — macro flip is expected for them
                is_counter_trend_trade = (
                    trade.side == TradeSide.LONG and trend.ema_50 < trend.ema_200
                ) or (
                    trade.side == TradeSide.SHORT and trend.ema_50 > trend.ema_200
                )
                if is_counter_trend_trade:
                    continue
    
                # Close when EMA50/EMA200 macro structure flips against trade direction
                should_close = (
                    trade.side == TradeSide.LONG and trend.ema_50 < trend.ema_200
                ) or (
                    trade.side == TradeSide.SHORT and trend.ema_50 > trend.ema_200
                )
    
                if not should_close:
                    continue
    
                logger.info(
                    "slope_sl_triggered",
                    trade_id=trade.id,
                    symbol=symbol,
                    side=trade.side.value,
                    ema_50=round(trend.ema_50, 2),
                    ema_200=round(trend.ema_200, 2),
                    reason="ema50_ema200_cross",
                )
    
                # Get mark price for P&L before closing
                slope_close_price: float | None = None
                if self._exchange and self._exchange.is_connected:
                    slope_close_price = self._exchange.get_mark_price(symbol)
                    self._exchange.cancel_all_orders(symbol)
                    if trade.market_type == MarketType.SPOT:
                        self._exchange.close_spot_position(symbol, trade.remaining_quantity)
                    else:
                        self._exchange.close_position(symbol)
    
                # Close trade in DB
                close_trade(trade, CloseReason.SLOPE_REVERSAL, close_price=slope_close_price)
                await self._database.update_trade_stage(trade, TradeStage.CLOSED)
    
                if self._notifier:
                    await self._notifier.notify_slope_close(trade, trend.ema_scaled)

    # --- Account Balance Snapshot (T / T-1 Tracker) ---

    async def _check_snapshot_due(self) -> None:
        """Check if it's time for a new account balance snapshot.

        Uses time-slot boundaries (not tick intervals) so the snapshot fires
        exactly once per period even if the monitor restarts mid-cycle.
        For daily (24h): fires when the UTC day changes.
        """
        now = datetime.now(UTC)
        interval = self._settings.snapshot_interval_hours
        current_slot = str(int(now.timestamp()) // (interval * 3600))

        if self._last_snapshot_slot == current_slot:
            return  # Already took snapshot for this period

        # Don't snapshot on first few ticks (let system stabilize)
        if self._tick_count < 10:
            return

        await self._take_account_snapshot()
        self._last_snapshot_slot = current_slot

    async def _take_account_snapshot(self) -> None:
        """Capture account equity and save snapshot with T-1 delta."""
        if not self._exchange or not self._exchange.is_connected:
            return

        equity = self._exchange.get_account_value()
        if equity is None:
            logger.warning("snapshot_equity_fetch_failed")
            return

        # Get previous snapshot for delta calculation
        prev = await self._database.get_latest_snapshot()

        if prev:
            delta = equity - prev.total_equity
            pnl_pct = (delta / prev.total_equity * 100) if prev.total_equity > 0 else 0.0
        else:
            delta = 0.0
            pnl_pct = 0.0

        snapshot = AccountSnapshot(
            total_equity=equity,
            equity_delta=round(delta, 2),
            pnl_pct=round(pnl_pct, 4),
        )

        await self._database.save_snapshot(snapshot)
        logger.info(
            "account_snapshot_saved",
            equity=equity,
            delta=round(delta, 2),
            pnl_pct=round(pnl_pct, 4),
        )

        if self._notifier:
            await self._notifier.notify_balance_snapshot(snapshot, prev)


# ---------------------------------------------------------------------------
# Standalone entry point (for systemd service)
# ---------------------------------------------------------------------------


async def _standalone_main() -> None:
    """Run the monitor as a standalone process.

    Used when the monitor runs as a separate systemd service
    rather than embedded inside the API process.
    """
    from app.config import get_settings
    from app.database import TradeDatabase
    from app.exchange import LBankExchangeError as ExchangeError, LBankClient as HyperliquidClient
    from app.logging_setup import setup_logging
    from app.telegram import TelegramNotifier

    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)
    logger.info("standalone_monitor_starting")

    database = TradeDatabase(settings.db_path)
    await database.init()

    # Expire stale signals on boot (Phase 2)
    expired = await database.expire_stale_signals(settings.signal_stale_ttl_minutes)
    if expired:
        logger.info("boot_stale_signals_expired", count=expired)

    exchange_client = None
    if settings.is_exchange_configured():
        try:
            exchange_client = HyperliquidClient(settings)
            exchange_client.connect()
            logger.info("exchange_client_ready")
        except ExchangeError as e:
            logger.error("exchange_connection_failed", error=str(e))

    notifier = TelegramNotifier(settings)
    monitor = TradeMonitor(settings, database, exchange_client, notifier=notifier)

    # Graceful shutdown on SIGTERM (systemctl stop) and SIGINT (Ctrl+C)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, monitor.request_stop)

    try:
        await monitor.run()
    finally:
        await database.close()
        logger.info("standalone_monitor_stopped")


def main() -> None:
    """Entry point for running monitor as a standalone service."""
    asyncio.run(_standalone_main())


if __name__ == "__main__":
    main()
