"""Telegram notification module for trade events.

Sends formatted messages to a Telegram chat via the Bot API.
All methods are no-ops when telegram_enabled=False.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx

from app.logging_setup import get_logger

if TYPE_CHECKING:
    from app.config import Settings
    from app.models import AccountSnapshot, Signal, Trade, TradeSide, TradeStage

logger = get_logger("app.telegram")

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:
    """Sends trade notifications to Telegram."""

    def __init__(self, settings: Settings) -> None:
        self._enabled = settings.telegram_enabled
        self._token = settings.telegram_bot_token
        self._chat_id = settings.telegram_chat_id
        self._client = httpx.AsyncClient(timeout=10.0) if self._enabled else None
        self._slope_label = (
            f"{settings.candle_timeframe} delta EMA slope"
            if settings.slope_method.value == "delta_ema_slope"
            else f"{settings.candle_timeframe} EMA slope"
        )

    async def send_message(self, text: str) -> None:
        """Send a plain-text message to the configured Telegram chat."""
        if not self._enabled or not self._client:
            return
        url = TELEGRAM_API_URL.format(token=self._token)
        try:
            resp = await self._client.post(
                url,
                json={
                    "chat_id": self._chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                },
            )
            if resp.status_code != 200:
                logger.warning(
                    "telegram_send_failed",
                    status=resp.status_code,
                    body=resp.text[:200],
                )
        except httpx.HTTPError as e:
            logger.warning("telegram_send_error", error=str(e))

    # --- Event-specific notifications ---

    async def notify_signal_created(self, signal: Signal) -> None:
        """New signal received from webhook."""
        await self.send_message(
            f"📡 <b>New Signal</b>\n"
            f"Symbol: {signal.symbol}\n"
            f"Action: {signal.action.value}\n"
            f"Price: {signal.entry_price}\n"
            f"Size: ${signal.size_usd}"
        )

    async def notify_trade_opened(
        self, trade: Trade, signal: Signal, *, is_counter_trend: bool = False, half_size_reason: str = "",
    ) -> None:
        """Signal approved and trade opened."""
        trend_label = "Counter-trend" if is_counter_trend else "Pro-trend"
        size_note = f"${signal.actual_size_usd:.0f}" if signal.actual_size_usd else f"${signal.size_usd:.0f}"
        if half_size_reason:
            size_note += f" (half: {half_size_reason})"

        # ATR regime line (only shown when regime detection is active)
        regime_line = ""
        if signal.is_fast_market is not None:
            regime_emoji = "⚡" if signal.is_fast_market else "🐢"
            regime_label = "FAST" if signal.is_fast_market else "SLOW"
            atr_pct = signal.atr_regime_pct if signal.atr_regime_pct is not None else 0.0
            regime_line = (
                f"\nRegime: {regime_emoji} {regime_label} (ATR {atr_pct:.1f}%)"
            )

        await self.send_message(
            f"✅ <b>Trade Opened</b>\n"
            f"Symbol: {trade.symbol} ({trade.side.value.upper()})\n"
            f"Trend: {trend_label}\n"
            f"Entry: {trade.entry_price:.2f}\n"
            f"SL: {trade.sl_price:.2f}\n"
            f"TP1: {trade.tp1_price:.2f} | TP2: {trade.tp2_price:.2f}\n"
            f"Size: {trade.quantity} ({size_note})\n"
            f"{self._slope_label}: {signal.ema_slope_value:.2f}\n"
            f"Slope rising: {signal.slope_rising}"
            f"{regime_line}"
        )

    async def notify_signal_rejected(self, signal: Signal) -> None:
        """Signal rejected (filters or trend)."""
        await self.send_message(
            f"❌ <b>Signal Rejected</b>\n"
            f"Symbol: {signal.symbol}\n"
            f"Status: {signal.status.value}\n"
            f"Reason: {signal.rejection_reason or 'N/A'}"
        )

    async def notify_signal_memory(self, signal: Signal) -> None:
        """Signal entered memory halt (trend misaligned, waiting)."""
        slope_str = (
            f"{signal.last_memory_slope:.4f}"
            if signal.last_memory_slope is not None
            else "N/A"
        )

        # ATR regime line
        regime_line = ""
        if signal.is_fast_market is not None:
            regime_emoji = "⚡" if signal.is_fast_market else "🐢"
            regime_label = "FAST" if signal.is_fast_market else "SLOW"
            atr_pct = signal.atr_regime_pct if signal.atr_regime_pct is not None else 0.0
            regime_line = f"\nRegime: {regime_emoji} {regime_label} (ATR {atr_pct:.1f}%)"

        await self.send_message(
            f"🧠 <b>Memory Halt</b>\n"
            f"Symbol: {signal.symbol}\n"
            f"{self._slope_label}: {slope_str}"
            f"{regime_line}\n"
            f"Waiting for slope recovery..."
        )

    async def notify_breakeven(self, trade: Trade, mark_price: float) -> None:
        """Trade SL moved to breakeven — position is now risk-free."""
        side = trade.side.value.upper()
        await self.send_message(
            f"\U0001f6e1 <b>Breakeven Triggered</b>\n"
            f"Symbol: {trade.symbol} ({side})\n"
            f"Entry: {trade.entry_price:.2f}\n"
            f"New SL: {trade.sl_price:.2f} (true BE incl. fees)\n"
            f"Mark: {mark_price:.2f}\n"
            f"Trade is now <b>risk-free</b>"
        )

    async def notify_partial_close(
        self, trade: Trade, mark_price: float, closed_size: float
    ) -> None:
        """50% of position closed at TP1 — shows secured profit."""
        from app.models import TradeSide

        side = trade.side.value.upper()
        if trade.side == TradeSide.LONG:
            tp1_pnl_usd = (mark_price - trade.entry_price) * closed_size
            tp1_pnl_pct = ((mark_price - trade.entry_price) / trade.entry_price) * 100
        else:
            tp1_pnl_usd = (trade.entry_price - mark_price) * closed_size
            tp1_pnl_pct = ((trade.entry_price - mark_price) / trade.entry_price) * 100

        sign = "+" if tp1_pnl_usd >= 0 else ""
        emoji = "\U0001f7e2" if tp1_pnl_usd >= 0 else "\U0001f534"

        await self.send_message(
            f"\U0001f4b0 <b>TP1 Partial Close</b>\n"
            f"Symbol: {trade.symbol} ({side})\n"
            f"Entry: {trade.entry_price:.2f}\n"
            f"Fill: {mark_price:.2f}\n"
            f"Closed: {closed_size} (50%)\n"
            f"Remaining: {trade.remaining_quantity}\n"
            f"SL: {trade.sl_price:.2f}\n"
            f"{emoji} Profit Secured: {sign}${tp1_pnl_usd:,.2f} ({sign}{tp1_pnl_pct:.2f}%)"
        )

    async def notify_trailing_activated(
        self, trade: Trade, mark_price: float
    ) -> None:
        """Trailing stop activated — shows current profit."""
        from app.models import TradeSide

        side = trade.side.value.upper()
        if trade.side == TradeSide.LONG:
            profit_pct = ((mark_price - trade.entry_price) / trade.entry_price) * 100
        else:
            profit_pct = ((trade.entry_price - mark_price) / trade.entry_price) * 100

        await self.send_message(
            f"\U0001f3af <b>Trailing Stop Activated</b>\n"
            f"Symbol: {trade.symbol} ({side})\n"
            f"Entry: {trade.entry_price:.2f}\n"
            f"Mark: {mark_price:.2f}\n"
            f"Profit: +{profit_pct:.2f}%\n"
            f"Trailing SL: {trade.sl_price:.2f}\n"
            f"Trailing is now <b>active</b>"
        )

    async def notify_sl_failed(self, trade: Trade, context: str) -> None:
        """CRITICAL: SL placement failed — trade may be unprotected."""
        side = trade.side.value.upper()
        await self.send_message(
            f"\U0001f6a8 <b>CRITICAL: SL PLACEMENT FAILED</b>\n"
            f"Symbol: {trade.symbol} ({side})\n"
            f"Trade ID: {trade.id[:8]}\n"
            f"Entry: {trade.entry_price:.2f}\n"
            f"Last known SL: {trade.sl_price:.2f}\n"
            f"Context: {context}\n"
            f"<b>Check exchange immediately!</b>"
        )

    def _calculate_pnl_breakdown(self, trade: Trade) -> str:
        """Calculate P&L breakdown accounting for partial exits.

        If TP1 partial close happened, shows:
          - TP1 leg P&L
          - Remaining leg P&L
          - Combined total
        Otherwise shows simple single-line P&L.
        """
        from app.models import TradeSide

        if trade.close_price is None:
            return ""

        is_long = trade.side == TradeSide.LONG
        entry = trade.entry_price
        close = trade.close_price

        if trade.partial_exit_done and trade.tp1_fill_price is not None:
            partial_qty = round(trade.quantity - trade.remaining_quantity, 8)

            if is_long:
                tp1_pnl = (trade.tp1_fill_price - entry) * partial_qty
                rem_pnl = (close - entry) * trade.remaining_quantity
            else:
                tp1_pnl = (entry - trade.tp1_fill_price) * partial_qty
                rem_pnl = (entry - close) * trade.remaining_quantity

            total_pnl = tp1_pnl + rem_pnl
            total_pct = (total_pnl / (entry * trade.quantity)) * 100

            sign_t = "+" if total_pnl >= 0 else ""
            sign_1 = "+" if tp1_pnl >= 0 else ""
            sign_r = "+" if rem_pnl >= 0 else ""
            emoji = "\U0001f7e2" if total_pnl >= 0 else "\U0001f534"

            return (
                f"\n--- P&L Breakdown ---"
                f"\nTP1 ({partial_qty}): {sign_1}${tp1_pnl:,.2f}"
                f"\nRemaining ({trade.remaining_quantity}): {sign_r}${rem_pnl:,.2f}"
                f"\n{emoji} Total: {sign_t}${total_pnl:,.2f} ({sign_t}{total_pct:.2f}%)"
            )
        else:
            qty = trade.remaining_quantity if trade.remaining_quantity > 0 else trade.quantity
            if is_long:
                pnl_usd = (close - entry) * qty
                pnl_pct = ((close - entry) / entry) * 100
            else:
                pnl_usd = (entry - close) * qty
                pnl_pct = ((entry - close) / entry) * 100

            sign = "+" if pnl_usd >= 0 else ""
            emoji = "\U0001f7e2" if pnl_usd >= 0 else "\U0001f534"
            return f"\n{emoji} P&L: {sign}${pnl_usd:,.2f} ({sign}{pnl_pct:.2f}%)"

    async def notify_trade_closed(self, trade: Trade) -> None:
        """Trade fully closed — includes SL level, trailing status, and P&L breakdown."""
        reason = trade.close_reason.value if trade.close_reason else "unknown"

        if trade.trailing_active:
            sl_type = "Trailing SL"
        elif trade.be_triggered:
            sl_type = "Breakeven SL"
        else:
            sl_type = "Initial SL"

        pnl_line = self._calculate_pnl_breakdown(trade)

        close_price_line = (
            f"\nClose: {trade.close_price:.2f}"
            if trade.close_price is not None
            else ""
        )

        await self.send_message(
            f"\U0001f3c1 <b>Trade Closed</b>\n"
            f"Symbol: {trade.symbol} ({trade.side.value.upper()})\n"
            f"Entry: {trade.entry_price:.2f}{close_price_line}\n"
            f"SL: {trade.sl_price:.2f} ({sl_type})\n"
            f"Reason: {reason}{pnl_line}"
        )

    async def notify_slope_close(self, trade: Trade, slope_value: float) -> None:
        """Trade closed by dynamic slope stop-loss."""
        pnl_line = self._calculate_pnl_breakdown(trade)

        close_price_line = (
            f"\nClose: {trade.close_price:.2f}"
            if trade.close_price is not None
            else ""
        )

        await self.send_message(
            f"\u26a0\ufe0f <b>Slope SL Triggered</b>\n"
            f"Symbol: {trade.symbol} ({trade.side.value.upper()})\n"
            f"Entry: {trade.entry_price:.2f}{close_price_line}\n"
            f"{self._slope_label}: {slope_value:.4f}\n"
            f"Emergency close executed{pnl_line}"
        )

    async def notify_balance_snapshot(
        self,
        snapshot: AccountSnapshot,
        previous: AccountSnapshot | None,
    ) -> None:
        """Daily account balance snapshot summary."""
        prev_equity = f"${previous.total_equity:,.2f}" if previous else "N/A"
        sign = "+" if snapshot.equity_delta >= 0 else ""
        arrow = "\U0001f4c8" if snapshot.equity_delta >= 0 else "\U0001f4c9"

        await self.send_message(
            f"\U0001f4b0 <b>Balance Snapshot</b>\n"
            f"T (Current): <b>${snapshot.total_equity:,.2f}</b>\n"
            f"T-1 (Previous): {prev_equity}\n"
            f"{arrow} Delta: {sign}${snapshot.equity_delta:,.2f} ({sign}{snapshot.pnl_pct:.2f}%)"
        )

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()