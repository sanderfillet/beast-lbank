"""Domain models for the TradingView → Hyperliquid bridge.

Defines the core data structures:
- TradeStage: The 5-stage trade lifecycle + CLOSED
- TradeSide: LONG or SHORT
- Trade: Full trade state persisted in the database
- WebhookPayload: Incoming TradingView alert structure
- TradeAction: Supported webhook action types
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator


class TradeStage(StrEnum):
    """Trade lifecycle stages.

    Flow: ENTRY → BREAKEVEN → PARTIAL_EXIT → TRAILING_ACTIVE → TRAILING_UPDATE → CLOSED

    ENTRY:            Order placed, initial SL set, waiting for price movement.
    BREAKEVEN:        Price reached TP1 zone — SL moved to entry price.
    PARTIAL_EXIT:     50% of position closed at TP1.
    TRAILING_ACTIVE:  Price reached TP2 — trailing stop activated.
    TRAILING_UPDATE:  Trailing stop ratcheting as price moves favorably.
    CLOSED:           Trade fully closed (hit SL, manually closed, or error).
    """

    ENTRY = "entry"
    BREAKEVEN = "breakeven"
    PARTIAL_EXIT = "partial_exit"
    TRAILING_ACTIVE = "trailing_active"
    TRAILING_UPDATE = "trailing_update"
    CLOSED = "closed"


# Valid stage transitions: from_stage -> set of allowed next stages
VALID_STAGE_TRANSITIONS: dict[TradeStage, set[TradeStage]] = {
    TradeStage.ENTRY: {TradeStage.BREAKEVEN, TradeStage.CLOSED},
    TradeStage.BREAKEVEN: {TradeStage.PARTIAL_EXIT, TradeStage.CLOSED},
    TradeStage.PARTIAL_EXIT: {TradeStage.TRAILING_ACTIVE, TradeStage.CLOSED},
    TradeStage.TRAILING_ACTIVE: {TradeStage.TRAILING_UPDATE, TradeStage.CLOSED},
    TradeStage.TRAILING_UPDATE: {TradeStage.TRAILING_UPDATE, TradeStage.CLOSED},
    TradeStage.CLOSED: set(),  # Terminal state — no transitions out
}


def is_valid_transition(from_stage: TradeStage, to_stage: TradeStage) -> bool:
    """Check if a stage transition is valid.

    Args:
        from_stage: Current stage.
        to_stage: Proposed next stage.

    Returns:
        True if the transition is allowed.
    """
    return to_stage in VALID_STAGE_TRANSITIONS.get(from_stage, set())


class TradeSide(StrEnum):
    """Trade direction."""

    LONG = "long"
    SHORT = "short"


class TradeAction(StrEnum):
    """Supported webhook action types from TradingView."""

    ENTRY_LONG = "entry_long"
    ENTRY_SHORT = "entry_short"
    CLOSE = "close"
    CLOSE_ALL = "close_all"


class CloseReason(StrEnum):
    """Why a trade was closed."""

    STOP_LOSS = "stop_loss"
    TRAILING_STOP = "trailing_stop"
    TAKE_PROFIT = "take_profit"
    MANUAL_CLOSE = "manual_close"
    WEBHOOK_CLOSE = "webhook_close"
    SLOPE_REVERSAL = "slope_reversal"
    DIRECTION_FLIP = "direction_flip"
    ERROR = "error"


class SignalStatus(StrEnum):
    """Status of a pending signal evaluation."""

    PENDING_EVAL = "pending_eval"
    PENDING_MEMORY = "pending_memory"
    APPROVED = "approved"
    REJECTED_TREND = "rejected_trend"
    REJECTED_POSITION = "rejected_position"
    REJECTED_FILTERS = "rejected_filters"
    EXPIRED_STALE = "expired_stale"
    EXPIRED_MACRO_BROKEN = "expired_macro_broken"
    ERROR = "error"

class MarketType(StrEnum):
    SPOT = "spot"
    PERP = "perp"

class ChaseAction(StrEnum):
    """Action when anti-chase filter triggers."""

    REJECT = "reject"
    HALF_SIZE = "half_size"


class Trade(BaseModel):
    """Full trade state — persisted in the database.

    This is the central data model. Each trade goes through the lifecycle
    stages and this model tracks all state needed for stage transitions.
    """

    # Identity
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = Field(..., description="Trading pair symbol (e.g., 'BTC', 'ETH')")
    side: TradeSide = Field(..., description="Trade direction: long or short")

    # Lifecycle
    stage: TradeStage = Field(
        default=TradeStage.ENTRY,
        description="Current trade lifecycle stage",
    )

    # Prices
    entry_price: float = Field(..., gt=0, description="Actual entry fill price")
    sl_price: float = Field(..., gt=0, description="Current stop-loss price")
    tp1_price: float = Field(
        ...,
        gt=0,
        description="Take-profit level 1 (breakeven + partial exit)",
    )
    tp2_price: float = Field(..., gt=0, description="Take-profit level 2 (trailing activation)")

    # Position
    quantity: float = Field(..., gt=0, description="Position size in contracts/coins")
    remaining_quantity: float = Field(
        default=0.0,
        ge=0,
        description="Remaining position after partial exits",
    )

    # Stage flags
    be_triggered: bool = Field(default=False, description="Whether breakeven has been triggered")
    dd_be_triggered: bool = Field(default=False, description="Whether drawdown BE has been triggered")
    partial_exit_done: bool = Field(default=False, description="Whether partial exit has executed")
    trailing_active: bool = Field(default=False, description="Whether trailing stop is active")
    trailing_offset: float = Field(
        default=0.0,
        ge=0,
        description="Trailing stop offset in price units",
    )
    highest_price: float = Field(
        default=0.0,
        ge=0,
        description="Highest price seen since trailing activated (for longs)",
    )
    lowest_price: float = Field(
        default=0.0,
        ge=0,
        description="Lowest price seen since trailing activated (for shorts)",
    )

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    closed_at: datetime | None = Field(default=None, description="When the trade was closed")
    close_reason: CloseReason | None = Field(default=None, description="Why the trade was closed")
    close_price: float | None = Field(default=None, description="Mark price at time of close")
    tp1_fill_price: float | None = Field(default=None, description="Mark price at TP1 partial close for P&L breakdown")

    # P&L tracking (populated on close)
    pnl_usd: float | None = Field(default=None, description="Realized P&L in USD (populated on trade close)")
    pnl_pct: float | None = Field(default=None, description="Realized P&L as percentage of entry (populated on trade close)")

    # Hyperliquid order IDs for tracking
    entry_order_id: str | None = Field(default=None, description="Exchange order ID for entry")
    sl_order_id: str | None = Field(default=None, description="Exchange order ID for stop-loss")
    tp1_order_id: str | None = Field(default=None, description="Exchange order ID for TP1 order")
    tp2_order_id: str | None = Field(default=None, description="Exchange order ID for TP2 order")

    # Phase 6: Brain evaluation data
    signal_id: str | None = Field(default=None, description="Linked signal ID")
    ema_slope_value: float | None = Field(default=None, description="EMA slope at entry eval")
    delta_slope_value: float | None = Field(default=None, description="Delta slope at entry eval")
    slope_rising: bool | None = Field(default=None, description="Whether EMA slope was rising at entry")
    atr_regime_pct: float | None = Field(default=None, description="ATR regime % at entry")
    is_fast_market: bool | None = Field(default=None, description="Whether ATR regime detected fast market at entry")

    market_type: MarketType = Field(default=MarketType.PERP)
    is_counter_trend: bool = Field(default=False, description="Whether this is a counter-trend trade (macro misaligned)")

    def model_post_init(self, __context: object) -> None:
        """Set remaining_quantity to full quantity if not explicitly set."""
        if self.remaining_quantity == 0.0:
            self.remaining_quantity = self.quantity

    @property
    def is_active(self) -> bool:
        """Whether this trade is still open (not closed)."""
        return self.stage != TradeStage.CLOSED

    @property
    def is_profitable(self) -> bool:
        """Check if current SL is above entry (for longs) or below (for shorts).

        This is a simple check — actual P&L depends on current mark price.
        """
        if self.side == TradeSide.LONG:
            return self.sl_price > self.entry_price
        return self.sl_price < self.entry_price


class WebhookPayload(BaseModel):
    """Incoming TradingView webhook alert payload.

    This matches the JSON structure sent from TradingView alerts.
    All price fields are strings because TradingView sends template values
    like {{close}} which resolve to string representations of numbers.
    """

    secret: str = Field(..., description="Webhook authentication secret")
    action: TradeAction = Field(..., description="What action to take")
    symbol: str = Field(..., description="Trading pair symbol (e.g., 'BTC', 'ETH')")
    signal_type: str | None = Field(
        default=None,
        description="Signal type for brain evaluation (e.g., 'liquidation_bubble')",
    )

    # Prices come as strings from TradingView templates like {{close}}
    entry_price: str | None = Field(default=None, description="Entry price (from TradingView)")
    sl_price: str | None = Field(default=None, description="Stop-loss price")
    tp1_price: str | None = Field(default=None, description="Take-profit 1 price")
    tp2_price: str | None = Field(default=None, description="Take-profit 2 price")

    # Optional override for trade size
    size_usd: str | None = Field(default=None, description="Trade size in USD (optional)")

    # Optional timestamp for stale signal TTL check
    timestamp: str | None = Field(default=None, description="Unix timestamp from alert source")

    # Market Type
    market_type: MarketType | None = Field(default=None)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        """Normalize symbol to base asset (e.g. 'BTCUSDT.P' -> 'BTC')."""
        s = v.upper().strip()
        # Strip trailing exchange suffixes
        for suffix in (".P", "USDT", "USDC", "USD", "PERP", "BUSD"):
            if s.endswith(suffix) and len(s) > len(suffix):
                s = s[: -len(suffix)]
        return s

    def get_entry_price(self) -> float | None:
        """Parse entry price to float, return None if not set."""
        return float(self.entry_price) if self.entry_price else None

    def get_sl_price(self) -> float | None:
        """Parse stop-loss price to float, return None if not set."""
        return float(self.sl_price) if self.sl_price else None

    def get_tp1_price(self) -> float | None:
        """Parse TP1 price to float, return None if not set."""
        return float(self.tp1_price) if self.tp1_price else None

    def get_tp2_price(self) -> float | None:
        """Parse TP2 price to float, return None if not set."""
        return float(self.tp2_price) if self.tp2_price else None

    def get_size_usd(self) -> float | None:
        """Parse trade size to float, return None if not set."""
        return float(self.size_usd) if self.size_usd else None


class Signal(BaseModel):
    """Pending signal awaiting evaluation by the Brain.

    Signals are created by the webhook and sit in pending_eval status
    until the monitor evaluates them after the configured delay.
    A signal may be promoted to a Trade or rejected.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    action: TradeAction
    signal_type: str = Field(default="liquidation_bubble")
    entry_price: float = Field(gt=0, description="Price at time of signal")
    size_usd: float = Field(gt=0, description="Requested trade size in USD")
    status: SignalStatus = Field(default=SignalStatus.PENDING_EVAL)

    # Evaluation results (populated after delay)
    ema_slope_value: float | None = Field(default=None, description="EMA scaled slope at eval (T)")
    ema_slope_prev: float | None = Field(default=None, description="EMA scaled slope previous candle (T-1)")
    delta_slope_value: float | None = Field(default=None, description="Delta slope at eval (T)")
    delta_slope_prev: float | None = Field(default=None, description="Delta slope previous candle (T-1)")
    slope_rising: bool | None = Field(default=None, description="Whether EMA slope was rising at eval")
    eval_price: float | None = Field(default=None, description="Price at evaluation time")
    actual_size_usd: float | None = Field(
        default=None,
        description="Size after value-zone adjustment",
    )
    rejection_reason: str | None = Field(default=None)
    trade_id: str | None = Field(default=None, description="Linked trade if approved")

    # Slope history (last 5 candles, oldest first, JSON in DB)
    ema_slope_history: list[float] | None = Field(default=None, description="Last 5 EMA slope values")
    delta_slope_history: list[float] | None = Field(default=None, description="Last 5 delta slope values")

    # ATR Regime data
    atr_regime_pct: float | None = Field(default=None, description="ATR regime % at eval time")
    is_fast_market: bool | None = Field(default=None, description="Whether ATR regime detected fast market")

    # Memory halt tracking (Phase 2)
    memory_entered_at: datetime | None = Field(default=None)
    last_memory_slope: float | None = Field(default=None)
    memory_eval_count: int = Field(default=0)

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    evaluated_at: datetime | None = Field(default=None)

    # Market Type
    market_type: MarketType = Field(default=MarketType.PERP)
    is_counter_trend: bool = Field(default=False, description="Whether this signal was evaluated as counter-trend")


class AccountSnapshot(BaseModel):
    """Daily account equity snapshot for T / T-1 tracking.

    Captures total equity at a regular interval (default: daily at 00:00 UTC)
    and records the delta from the previous snapshot so the team can monitor
    the account's equity curve over time.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    total_equity: float = Field(ge=0, description="Total account value in USD")
    equity_delta: float = Field(default=0.0, description="Change from previous snapshot")
    pnl_pct: float = Field(default=0.0, description="Percent change from previous snapshot")
