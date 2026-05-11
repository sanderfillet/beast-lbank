"""SQLite persistence layer for trade state.

Uses aiosqlite for async database operations. The database is designed
to survive restarts — all active trade state is persisted so the monitor
loop can resume managing trades after a crash or restart.

Usage:
    db = TradeDatabase("trades.db")
    await db.init()
    await db.save_trade(trade)
    active = await db.get_active_trades()
    await db.close()
"""

from __future__ import annotations

import contextlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import aiosqlite  # pyright: ignore[reportMissingImports]

from app.logging_setup import get_logger
from app.models import (
    AccountSnapshot,
    CloseReason,
    MarketType,
    Signal,
    SignalStatus,
    Trade,
    TradeAction,
    TradeSide,
    TradeStage,
    is_valid_transition,
)

logger = get_logger("app.database")

# SQL for creating the trades table
CREATE_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT 'entry',
    entry_price REAL NOT NULL,
    sl_price REAL NOT NULL,
    tp1_price REAL NOT NULL,
    tp2_price REAL NOT NULL,
    quantity REAL NOT NULL,
    remaining_quantity REAL NOT NULL,
    be_triggered INTEGER NOT NULL DEFAULT 0,
    dd_be_triggered INTEGER NOT NULL DEFAULT 0,
    partial_exit_done INTEGER NOT NULL DEFAULT 0,
    trailing_active INTEGER NOT NULL DEFAULT 0,
    trailing_offset REAL NOT NULL DEFAULT 0.0,
    highest_price REAL NOT NULL DEFAULT 0.0,
    lowest_price REAL NOT NULL DEFAULT 0.0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_at TEXT,
    close_reason TEXT,
    tp1_fill_price REAL,
    entry_order_id TEXT,
    sl_order_id TEXT,
    tp1_order_id TEXT,
    tp2_order_id TEXT,
    trade_unit_id TEXT,
    signal_id TEXT,
    ema_slope_value REAL,
    delta_slope_value REAL,
    slope_rising INTEGER,
    market_type TEXT NOT NULL DEFAULT 'perp'
);
"""

CREATE_TRADES_INDEX = """
CREATE INDEX IF NOT EXISTS idx_trades_stage ON trades(stage);
"""

# Deduplication table for webhook idempotency
CREATE_DEDUP_TABLE = """
CREATE TABLE IF NOT EXISTS webhook_dedup (
    dedup_key TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);
"""

CREATE_DEDUP_INDEX = """
CREATE INDEX IF NOT EXISTS idx_dedup_created ON webhook_dedup(created_at);
"""

# Signals table for Phase 6 (The Brain)
CREATE_SIGNALS_TABLE = """
CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    action TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    entry_price REAL NOT NULL,
    size_usd REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending_eval',
    ema_slope_value REAL,
    ema_slope_prev REAL,
    delta_slope_value REAL,
    delta_slope_prev REAL,
    slope_rising INTEGER,
    eval_price REAL,
    actual_size_usd REAL,
    rejection_reason TEXT,
    trade_id TEXT,
    ema_slope_history TEXT,
    delta_slope_history TEXT,
    created_at TEXT NOT NULL,
    evaluated_at TEXT
);
"""

CREATE_SIGNALS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
"""

CREATE_SIGNALS_CREATED_INDEX = """
CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at);
"""

# Account balance snapshots table (T / T-1 tracker)
CREATE_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS account_snapshots (
    id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    total_equity REAL NOT NULL,
    equity_delta REAL NOT NULL DEFAULT 0.0,
    pnl_pct REAL NOT NULL DEFAULT 0.0
);
"""

CREATE_SNAPSHOTS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON account_snapshots(timestamp);
"""

# Migration for existing trades tables (adds Phase 6 columns)
TRADE_MIGRATIONS = [
    "ALTER TABLE trades ADD COLUMN signal_id TEXT",
    "ALTER TABLE trades ADD COLUMN ema_slope_value REAL",
    "ALTER TABLE trades ADD COLUMN delta_slope_value REAL",
    "ALTER TABLE trades ADD COLUMN slope_rising INTEGER",
    "ALTER TABLE trades ADD COLUMN close_price REAL",
    "ALTER TABLE trades ADD COLUMN tp1_fill_price REAL",
    "ALTER TABLE trades ADD COLUMN pnl_usd REAL",
    "ALTER TABLE trades ADD COLUMN pnl_pct REAL",
    "ALTER TABLE trades ADD COLUMN atr_regime_pct REAL",
    "ALTER TABLE trades ADD COLUMN is_fast_market INTEGER",
    "ALTER TABLE trades ADD COLUMN market_type TEXT NOT NULL DEFAULT 'perp'",
]

# Migration for existing signals table (adds Phase 2 columns)
SIGNAL_MIGRATIONS = [
    "ALTER TABLE signals ADD COLUMN memory_entered_at TEXT",
    "ALTER TABLE signals ADD COLUMN last_memory_slope REAL",
    "ALTER TABLE signals ADD COLUMN memory_eval_count INTEGER DEFAULT 0",
    "ALTER TABLE signals ADD COLUMN ema_slope_prev REAL",
    "ALTER TABLE signals ADD COLUMN delta_slope_prev REAL",
    "ALTER TABLE signals ADD COLUMN slope_rising INTEGER",
    "ALTER TABLE signals ADD COLUMN ema_slope_history TEXT",
    "ALTER TABLE signals ADD COLUMN delta_slope_history TEXT",
    "ALTER TABLE signals ADD COLUMN atr_regime_pct REAL",
    "ALTER TABLE signals ADD COLUMN is_fast_market INTEGER",
    "ALTER TABLE signals ADD COLUMN market_type TEXT NOT NULL DEFAULT 'perp'",
    "ALTER TABLE trades ADD COLUMN is_counter_trend INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE trades ADD COLUMN trade_unit_id TEXT",
    "ALTER TABLE trades ADD COLUMN dd_be_triggered INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE signals ADD COLUMN is_counter_trend INTEGER NOT NULL DEFAULT 0",
]


class TradeDatabase:
    """Async SQLite database for trade persistence."""

    def __init__(self, db_path: str | Path = "trades.db") -> None:
        self.db_path = str(db_path)
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        """Initialize the database connection and create tables."""
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA foreign_keys=ON;")
        await self._db.execute(CREATE_TRADES_TABLE)
        await self._db.execute(CREATE_TRADES_INDEX)
        await self._db.execute(CREATE_DEDUP_TABLE)
        await self._db.execute(CREATE_DEDUP_INDEX)
        await self._db.execute(CREATE_SIGNALS_TABLE)
        await self._db.execute(CREATE_SIGNALS_INDEX)
        await self._db.execute(CREATE_SIGNALS_CREATED_INDEX)
        await self._db.execute(CREATE_SNAPSHOTS_TABLE)
        await self._db.execute(CREATE_SNAPSHOTS_INDEX)

        for migration in TRADE_MIGRATIONS:
            with contextlib.suppress(Exception):
                await self._db.execute(migration)
        for migration in SIGNAL_MIGRATIONS:
            with contextlib.suppress(Exception):
                await self._db.execute(migration)
        await self._db.commit()
        logger.info("database_initialized", path=self.db_path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None
            logger.info("database_closed")

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            msg = "Database not initialized. Call init() first."
            raise RuntimeError(msg)
        return self._db

    # --- Create / Update ---

    async def save_trade(self, trade: Trade) -> None:
        """Insert a new trade into the database."""
        await self.db.execute(
            """
            INSERT INTO trades (
                id, symbol, side, stage, entry_price, sl_price, tp1_price, tp2_price,
                quantity, remaining_quantity, be_triggered, dd_be_triggered, partial_exit_done,
                trailing_active, trailing_offset, highest_price, lowest_price,
                created_at, updated_at, closed_at, close_reason, close_price,
                tp1_fill_price, pnl_usd, pnl_pct,
                entry_order_id, sl_order_id, tp1_order_id, tp2_order_id,
                signal_id, ema_slope_value, delta_slope_value, slope_rising,
                atr_regime_pct, is_fast_market, market_type, is_counter_trend,
                trade_unit_id
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?
            )
            """,
            _trade_to_row(trade),
        )
        await self.db.commit()
        logger.info("trade_saved", trade_id=trade.id, symbol=trade.symbol, side=trade.side.value)

    async def update_trade(self, trade: Trade) -> None:
        """Update an existing trade in the database."""
        trade.updated_at = datetime.now(UTC)
        await self.db.execute(
            """
            UPDATE trades SET
                stage = ?, sl_price = ?, remaining_quantity = ?,
                be_triggered = ?, dd_be_triggered = ?, partial_exit_done = ?,
                trailing_active = ?, trailing_offset = ?,
                highest_price = ?, lowest_price = ?,
                updated_at = ?, closed_at = ?, close_reason = ?,
                close_price = ?, tp1_fill_price = ?,
                pnl_usd = ?, pnl_pct = ?,
                entry_order_id = ?, sl_order_id = ?,
                tp1_order_id = ?, tp2_order_id = ?,
                is_counter_trend = ?,
                trade_unit_id = ?,
                tp1_price = ?,
		tp2_price = ?
            WHERE id = ?
            """,
            (
                trade.stage.value,
                trade.sl_price,
                trade.remaining_quantity,
                int(trade.be_triggered),
                int(trade.dd_be_triggered),
                int(trade.partial_exit_done),
                int(trade.trailing_active),
                trade.trailing_offset,
                trade.highest_price,
                trade.lowest_price,
                trade.updated_at.isoformat(),
                trade.closed_at.isoformat() if trade.closed_at else None,
                trade.close_reason.value if trade.close_reason else None,
                trade.close_price,
                trade.tp1_fill_price,
                trade.pnl_usd,
                trade.pnl_pct,
                trade.entry_order_id,
                trade.sl_order_id,
                trade.tp1_order_id,
                trade.tp2_order_id,
                int(trade.is_counter_trend),
                trade.trade_unit_id,
                trade.tp1_price,
		trade.tp2_price,
                trade.id,
            ),
        )
        await self.db.commit()
        logger.debug("trade_updated", trade_id=trade.id, stage=trade.stage.value)

    async def update_trade_stage(self, trade: Trade, new_stage: TradeStage) -> bool:
        """Atomically validate and update a trade's stage."""
        cursor = await self.db.execute(
            "SELECT stage FROM trades WHERE id = ?", (trade.id,)
        )
        row = await cursor.fetchone()
        if row is None:
            logger.error("stage_update_trade_not_found", trade_id=trade.id)
            return False

        current_stage = TradeStage(row["stage"])
        if not is_valid_transition(current_stage, new_stage):
            logger.warning(
                "invalid_stage_transition",
                trade_id=trade.id,
                current_stage=current_stage.value,
                proposed_stage=new_stage.value,
            )
            return False

        trade.stage = new_stage
        await self.update_trade(trade)
        logger.info(
            "stage_transition",
            trade_id=trade.id,
            from_stage=current_stage.value,
            to_stage=new_stage.value,
        )
        return True

    # --- Read ---

    async def get_trade(self, trade_id: str) -> Trade | None:
        cursor = await self.db.execute("SELECT * FROM trades WHERE id = ?", (trade_id,))
        row = await cursor.fetchone()
        return _row_to_trade(row) if row else None

    async def get_active_trades(self) -> list[Trade]:
        cursor = await self.db.execute(
            "SELECT * FROM trades WHERE stage != ? ORDER BY created_at ASC",
            (TradeStage.CLOSED.value,),
        )
        rows = await cursor.fetchall()
        return [_row_to_trade(row) for row in rows]

    async def get_trades_by_symbol(self, symbol: str) -> list[Trade]:
        cursor = await self.db.execute(
            "SELECT * FROM trades WHERE symbol = ? AND stage != ? ORDER BY created_at ASC",
            (symbol.upper(), TradeStage.CLOSED.value),
        )
        rows = await cursor.fetchall()
        return [_row_to_trade(row) for row in rows]

    async def get_last_closed_trade(self, symbol: str) -> Trade | None:
        cursor = await self.db.execute(
            "SELECT * FROM trades WHERE symbol = ? AND stage = ? ORDER BY closed_at DESC LIMIT 1",
            (symbol.upper(), TradeStage.CLOSED.value),
        )
        row = await cursor.fetchone()
        return _row_to_trade(row) if row else None

    async def get_all_trades(self, limit: int = 100) -> list[Trade]:
        cursor = await self.db.execute(
            "SELECT * FROM trades ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [_row_to_trade(row) for row in rows]

    # --- Delete (for testing) ---

    async def delete_trade(self, trade_id: str) -> bool:
        cursor = await self.db.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
        await self.db.commit()
        return cursor.rowcount > 0

    # --- Stats ---

    async def count_active_trades(self) -> int:
        cursor = await self.db.execute(
            "SELECT COUNT(*) FROM trades WHERE stage != ?", (TradeStage.CLOSED.value,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    # --- Signal CRUD ---

    async def save_signal(self, signal: Signal) -> None:
        await self.db.execute(
            """
            INSERT INTO signals (
                id, symbol, action, signal_type, entry_price, size_usd, status,
                ema_slope_value, ema_slope_prev, delta_slope_value, delta_slope_prev,
                slope_rising, eval_price, actual_size_usd,
                rejection_reason, trade_id, ema_slope_history, delta_slope_history,
                created_at, evaluated_at,
                memory_entered_at, last_memory_slope, memory_eval_count,
                atr_regime_pct, is_fast_market, market_type, is_counter_trend
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            _signal_to_row(signal),
        )
        await self.db.commit()
        logger.info("signal_saved", signal_id=signal.id, symbol=signal.symbol)

    async def update_signal(self, signal: Signal) -> None:
        await self.db.execute(
            """
            UPDATE signals SET
                status = ?, ema_slope_value = ?, ema_slope_prev = ?,
                delta_slope_value = ?, delta_slope_prev = ?, slope_rising = ?,
                eval_price = ?, actual_size_usd = ?,
                rejection_reason = ?, trade_id = ?,
                ema_slope_history = ?, delta_slope_history = ?,
                evaluated_at = ?,
                memory_entered_at = ?, last_memory_slope = ?,
                memory_eval_count = ?, created_at = ?,
                atr_regime_pct = ?, is_fast_market = ?,
                is_counter_trend = ?
            WHERE id = ?
            """,
            (
                signal.status.value,
                signal.ema_slope_value,
                signal.ema_slope_prev,
                signal.delta_slope_value,
                signal.delta_slope_prev,
                int(signal.slope_rising) if signal.slope_rising is not None else None,
                signal.eval_price,
                signal.actual_size_usd,
                signal.rejection_reason,
                signal.trade_id,
                json.dumps(signal.ema_slope_history) if signal.ema_slope_history else None,
                json.dumps(signal.delta_slope_history) if signal.delta_slope_history else None,
                signal.evaluated_at.isoformat() if signal.evaluated_at else None,
                signal.memory_entered_at.isoformat() if signal.memory_entered_at else None,
                signal.last_memory_slope,
                signal.memory_eval_count,
                signal.created_at.isoformat(),
                signal.atr_regime_pct,
                int(signal.is_fast_market) if signal.is_fast_market is not None else None,
                int(signal.is_counter_trend),
                signal.id,
            ),
        )
        await self.db.commit()

    async def get_pending_signals(self) -> list[Signal]:
        cursor = await self.db.execute(
            "SELECT * FROM signals WHERE status = ? ORDER BY created_at ASC",
            (SignalStatus.PENDING_EVAL.value,),
        )
        rows = await cursor.fetchall()
        return [_row_to_signal(row) for row in rows]

    async def get_signal(self, signal_id: str) -> Signal | None:
        cursor = await self.db.execute("SELECT * FROM signals WHERE id = ?", (signal_id,))
        row = await cursor.fetchone()
        return _row_to_signal(row) if row else None

    async def get_all_signals(self, limit: int = 100) -> list[Signal]:
        cursor = await self.db.execute(
            "SELECT * FROM signals ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [_row_to_signal(row) for row in rows]

    async def get_recent_signal(self, symbol: str, action: str, after: datetime) -> Signal | None:
        cursor = await self.db.execute(
            "SELECT * FROM signals WHERE symbol = ? AND action = ? AND created_at > ? "
            "AND status IN (?, ?) ORDER BY created_at DESC LIMIT 1",
            (
                symbol.upper(),
                action,
                after.isoformat(),
                SignalStatus.PENDING_EVAL.value,
                SignalStatus.PENDING_MEMORY.value,
            ),
        )
        row = await cursor.fetchone()
        return _row_to_signal(row) if row else None

    # --- Phase 2: Memory Halt & Queue Management ---

    async def expire_stale_signals(self, ttl_minutes: int) -> int:
        from datetime import timedelta
        cutoff = (datetime.now(UTC) - timedelta(minutes=ttl_minutes)).isoformat()
        cursor = await self.db.execute(
            """
            UPDATE signals SET status = ?, rejection_reason = 'stale_ttl_expired'
            WHERE status IN (?, ?) AND created_at < ?
            """,
            (
                SignalStatus.EXPIRED_STALE.value,
                SignalStatus.PENDING_EVAL.value,
                SignalStatus.PENDING_MEMORY.value,
                cutoff,
            ),
        )
        await self.db.commit()
        count = cursor.rowcount
        if count > 0:
            logger.info("stale_signals_expired", count=count, ttl_minutes=ttl_minutes)
        return count

    async def get_memory_signals(self) -> list[Signal]:
        cursor = await self.db.execute(
            "SELECT * FROM signals WHERE status = ? ORDER BY created_at ASC",
            (SignalStatus.PENDING_MEMORY.value,),
        )
        rows = await cursor.fetchall()
        return [_row_to_signal(row) for row in rows]

    async def get_pending_signals_by_symbol(self, symbol: str) -> list[Signal]:
        cursor = await self.db.execute(
            """
            SELECT * FROM signals
            WHERE symbol = ? AND status IN (?, ?)
            ORDER BY created_at ASC
            """,
            (
                symbol.upper(),
                SignalStatus.PENDING_EVAL.value,
                SignalStatus.PENDING_MEMORY.value,
            ),
        )
        rows = await cursor.fetchall()
        return [_row_to_signal(row) for row in rows]

    # --- Account Snapshots ---

    async def save_snapshot(self, snapshot: AccountSnapshot) -> None:
        await self.db.execute(
            """
            INSERT INTO account_snapshots (id, timestamp, total_equity, equity_delta, pnl_pct)
            VALUES (?, ?, ?, ?, ?)
            """,
            _snapshot_to_row(snapshot),
        )
        await self.db.commit()
        logger.info(
            "snapshot_saved",
            snapshot_id=snapshot.id,
            equity=snapshot.total_equity,
            delta=snapshot.equity_delta,
        )

    async def get_latest_snapshot(self) -> AccountSnapshot | None:
        cursor = await self.db.execute(
            "SELECT * FROM account_snapshots ORDER BY timestamp DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        return _row_to_snapshot(row) if row else None

    async def get_snapshots(self, limit: int = 30) -> list[AccountSnapshot]:
        cursor = await self.db.execute(
            "SELECT * FROM account_snapshots ORDER BY timestamp DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [_row_to_snapshot(row) for row in rows]

    # --- Webhook Deduplication ---

    async def check_and_store_dedup(self, dedup_key: str) -> bool:
        cursor = await self.db.execute(
            "SELECT 1 FROM webhook_dedup WHERE dedup_key = ?", (dedup_key,)
        )
        if await cursor.fetchone():
            return True
        await self.db.execute(
            "INSERT INTO webhook_dedup (dedup_key, created_at) VALUES (?, ?)",
            (dedup_key, datetime.now(UTC).isoformat()),
        )
        await self.db.commit()
        return False

    async def cleanup_dedup(self, ttl_seconds: int = 300) -> int:
        cutoff = datetime.now(UTC).isoformat()
        cursor = await self.db.execute(
            "DELETE FROM webhook_dedup WHERE created_at < datetime(?, ?)",
            (cutoff, f"-{ttl_seconds} seconds"),
        )
        await self.db.commit()
        return cursor.rowcount


# --- Row conversion helpers ---


def _trade_to_row(trade: Trade) -> tuple:
    """Convert a Trade model to a database row tuple."""
    return (
        trade.id,
        trade.symbol,
        trade.side.value,
        trade.stage.value,
        trade.entry_price,
        trade.sl_price,
        trade.tp1_price,
        trade.tp2_price,
        trade.quantity,
        trade.remaining_quantity,
        int(trade.be_triggered),
        int(trade.dd_be_triggered),
        int(trade.partial_exit_done),
        int(trade.trailing_active),
        trade.trailing_offset,
        trade.highest_price,
        trade.lowest_price,
        trade.created_at.isoformat(),
        trade.updated_at.isoformat(),
        trade.closed_at.isoformat() if trade.closed_at else None,
        trade.close_reason.value if trade.close_reason else None,
        trade.close_price,
        trade.tp1_fill_price,
        trade.pnl_usd,
        trade.pnl_pct,
        trade.entry_order_id,
        trade.sl_order_id,
        trade.tp1_order_id,
        trade.tp2_order_id,
        trade.signal_id,
        trade.ema_slope_value,
        trade.delta_slope_value,
        int(trade.slope_rising) if trade.slope_rising is not None else None,
        trade.atr_regime_pct,
        int(trade.is_fast_market) if trade.is_fast_market is not None else None,
        trade.market_type.value,
        int(trade.is_counter_trend),
        trade.trade_unit_id,
    )


def _row_to_trade(row: aiosqlite.Row) -> Trade:
    """Convert a database row to a Trade model."""
    return Trade(
        id=row["id"],
        symbol=row["symbol"],
        side=TradeSide(row["side"]),
        stage=TradeStage(row["stage"]),
        entry_price=row["entry_price"],
        sl_price=row["sl_price"],
        tp1_price=row["tp1_price"],
        tp2_price=row["tp2_price"],
        quantity=row["quantity"],
        remaining_quantity=row["remaining_quantity"],
        be_triggered=bool(row["be_triggered"]),
        dd_be_triggered=bool(row["dd_be_triggered"]) if "dd_be_triggered" in row.keys() else False,
        partial_exit_done=bool(row["partial_exit_done"]),
        trailing_active=bool(row["trailing_active"]),
        trailing_offset=row["trailing_offset"],
        highest_price=row["highest_price"],
        lowest_price=row["lowest_price"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        closed_at=datetime.fromisoformat(row["closed_at"]) if row["closed_at"] else None,
        close_reason=CloseReason(row["close_reason"]) if row["close_reason"] else None,
        close_price=row["close_price"] if "close_price" in row.keys() else None,
        tp1_fill_price=row["tp1_fill_price"] if "tp1_fill_price" in row.keys() else None,
        pnl_usd=row["pnl_usd"] if "pnl_usd" in row.keys() else None,
        pnl_pct=row["pnl_pct"] if "pnl_pct" in row.keys() else None,
        entry_order_id=row["entry_order_id"],
        sl_order_id=row["sl_order_id"],
        tp1_order_id=row["tp1_order_id"],
        tp2_order_id=row["tp2_order_id"],
        signal_id=row["signal_id"],
        ema_slope_value=row["ema_slope_value"],
        delta_slope_value=row["delta_slope_value"],
        slope_rising=bool(row["slope_rising"]) if row["slope_rising"] is not None else None,
        atr_regime_pct=row["atr_regime_pct"] if "atr_regime_pct" in row.keys() else None,
        is_fast_market=bool(row["is_fast_market"]) if "is_fast_market" in row.keys() and row["is_fast_market"] is not None else None,
        market_type=MarketType(row["market_type"]) if "market_type" in row.keys() else MarketType.PERP,
        is_counter_trend=bool(row["is_counter_trend"]) if "is_counter_trend" in row.keys() and row["is_counter_trend"] is not None else False,
        trade_unit_id=row["trade_unit_id"] if "trade_unit_id" in row.keys() else None,
    )


def _signal_to_row(signal: Signal) -> tuple:
    """Convert a Signal model to a database row tuple."""
    return (
        signal.id,
        signal.symbol,
        signal.action.value,
        signal.signal_type,
        signal.entry_price,
        signal.size_usd,
        signal.status.value,
        signal.ema_slope_value,
        signal.ema_slope_prev,
        signal.delta_slope_value,
        signal.delta_slope_prev,
        int(signal.slope_rising) if signal.slope_rising is not None else None,
        signal.eval_price,
        signal.actual_size_usd,
        signal.rejection_reason,
        signal.trade_id,
        json.dumps(signal.ema_slope_history) if signal.ema_slope_history else None,
        json.dumps(signal.delta_slope_history) if signal.delta_slope_history else None,
        signal.created_at.isoformat(),
        signal.evaluated_at.isoformat() if signal.evaluated_at else None,
        signal.memory_entered_at.isoformat() if signal.memory_entered_at else None,
        signal.last_memory_slope,
        signal.memory_eval_count,
        signal.atr_regime_pct,
        int(signal.is_fast_market) if signal.is_fast_market is not None else None,
        signal.market_type.value,
        int(signal.is_counter_trend),
    )


def _row_to_signal(row: aiosqlite.Row) -> Signal:
    """Convert a database row to a Signal model."""
    memory_entered_at = None
    last_memory_slope = None
    memory_eval_count = 0
    with contextlib.suppress(IndexError, KeyError):
        memory_entered_at = (
            datetime.fromisoformat(row["memory_entered_at"]) if row["memory_entered_at"] else None
        )
    with contextlib.suppress(IndexError, KeyError):
        last_memory_slope = row["last_memory_slope"]
    with contextlib.suppress(IndexError, KeyError):
        memory_eval_count = row["memory_eval_count"] or 0

    ema_slope_prev = None
    delta_slope_prev = None
    slope_rising = None
    with contextlib.suppress(IndexError, KeyError):
        ema_slope_prev = row["ema_slope_prev"]
    with contextlib.suppress(IndexError, KeyError):
        delta_slope_prev = row["delta_slope_prev"]
    with contextlib.suppress(IndexError, KeyError):
        slope_rising = bool(row["slope_rising"]) if row["slope_rising"] is not None else None

    ema_slope_history = None
    delta_slope_history = None
    with contextlib.suppress(IndexError, KeyError):
        ema_slope_history = json.loads(row["ema_slope_history"]) if row["ema_slope_history"] else None
    with contextlib.suppress(IndexError, KeyError):
        delta_slope_history = json.loads(row["delta_slope_history"]) if row["delta_slope_history"] else None

    atr_regime_pct = None
    is_fast_market = None
    is_counter_trend = False
    with contextlib.suppress(IndexError, KeyError):
        is_counter_trend = bool(row["is_counter_trend"]) if row["is_counter_trend"] is not None else False
    with contextlib.suppress(IndexError, KeyError):
        atr_regime_pct = row["atr_regime_pct"]
    with contextlib.suppress(IndexError, KeyError):
        is_fast_market = bool(row["is_fast_market"]) if row["is_fast_market"] is not None else None

    market_type = (
        MarketType(row["market_type"]) if "market_type" in row.keys() else MarketType.PERP
    )

    return Signal(
        id=row["id"],
        symbol=row["symbol"],
        action=TradeAction(row["action"]),
        signal_type=row["signal_type"],
        entry_price=row["entry_price"],
        size_usd=row["size_usd"],
        status=SignalStatus(row["status"]),
        ema_slope_value=row["ema_slope_value"],
        ema_slope_prev=ema_slope_prev,
        delta_slope_value=row["delta_slope_value"],
        delta_slope_prev=delta_slope_prev,
        slope_rising=slope_rising,
        eval_price=row["eval_price"],
        actual_size_usd=row["actual_size_usd"],
        rejection_reason=row["rejection_reason"],
        trade_id=row["trade_id"],
        ema_slope_history=ema_slope_history,
        delta_slope_history=delta_slope_history,
        created_at=datetime.fromisoformat(row["created_at"]),
        evaluated_at=(datetime.fromisoformat(row["evaluated_at"]) if row["evaluated_at"] else None),
        atr_regime_pct=atr_regime_pct,
        is_fast_market=is_fast_market,
        memory_entered_at=memory_entered_at,
        last_memory_slope=last_memory_slope,
        memory_eval_count=memory_eval_count,
        market_type=market_type,
    )


def _snapshot_to_row(snapshot: AccountSnapshot) -> tuple:
    return (
        snapshot.id,
        snapshot.timestamp.isoformat(),
        snapshot.total_equity,
        snapshot.equity_delta,
        snapshot.pnl_pct,
    )


def _row_to_snapshot(row: aiosqlite.Row) -> AccountSnapshot:
    return AccountSnapshot(
        id=row["id"],
        timestamp=datetime.fromisoformat(row["timestamp"]),
        total_equity=row["total_equity"],
        equity_delta=row["equity_delta"],
        pnl_pct=row["pnl_pct"],
    )
