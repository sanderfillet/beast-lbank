"""The Brain — Sander's entry logic for signal evaluation.

Translates Sander's Pine Script MTF trend math into Pandas and provides:
- CCXT candle fetching (configurable timeframe, default 5m)
- Trend filter calculation (EMA-120 scaled slope + delta slope)
- Trend alignment check (direction must match signal)
- Value zone sizing (full size in value zone, half outside)
- Dynamic exit price computation from entry + config percentages

Used by the monitor loop to evaluate pending signals after the
25-minute delay.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import ccxt
import pandas as pd

from app.logging_setup import get_logger

if TYPE_CHECKING:
    from app.config import Settings

logger = get_logger("app.brain")

# ---------------------------------------------------------------------------
# Symbol mapping per exchange
# ---------------------------------------------------------------------------

_SYMBOL_MAP: dict[str, dict[str, str]] = {
    "binance": {
        "default_suffix": "/USDT:USDT",
    },
    "hyperliquid": {
        "default_suffix": "/USDC:USDC",
    },
}


def _map_symbol(symbol: str, exchange_source: str) -> str:
    """Map a simple symbol (e.g. 'BTC') to the exchange's futures format.

    Args:
        symbol: Simple symbol like 'BTC' or 'ETH'.
        exchange_source: CCXT exchange name (e.g. 'binance').

    Returns:
        Exchange-specific symbol like 'BTC/USDT:USDT'.
    """
    mapping = _SYMBOL_MAP.get(exchange_source, {})
    suffix = mapping.get("default_suffix", "/USDT:USDT")
    return f"{symbol}{suffix}"


# ---------------------------------------------------------------------------
# CCXT Candle Fetching
# ---------------------------------------------------------------------------


async def fetch_candles(
    symbol: str,
    exchange_source: str = "binance",
    limit: int = 1500,
    timeframe: str = "5m",
) -> pd.DataFrame:
    """Fetch OHLCV candles via CCXT at the configured timeframe.

    Args:
        symbol: Simple symbol (e.g. 'BTC').
        exchange_source: CCXT exchange ID (default: binance).
        limit: Number of candles to fetch (default: 1500 for EMA-200 warm-up convergence).
        timeframe: Candle timeframe (default: '5m'). Common values: '5m', '15m', '1h'.

    Returns:
        DataFrame with columns: timestamp, open, high, low, close, volume.

    Raises:
        RuntimeError: If fetching fails.
    """
    ccxt_symbol = _map_symbol(symbol, exchange_source)

    logger.info(
        "fetching_candles",
        symbol=symbol,
        ccxt_symbol=ccxt_symbol,
        exchange=exchange_source,
        timeframe=timeframe,
        limit=limit,
    )

    try:
        exchange_cls = getattr(ccxt, exchange_source)
        exchange = exchange_cls({"enableRateLimit": True})

        # Binance caps at 1000 candles per request — paginate if more needed
        max_per_request = 1000
        if limit <= max_per_request:
            ohlcv = exchange.fetch_ohlcv(ccxt_symbol, timeframe=timeframe, limit=limit)
        else:
            # Fetch in batches, walking backward from now
            all_candles: list = []
            remaining = limit
            since = None  # Start from latest

            # First fetch (most recent)
            batch = exchange.fetch_ohlcv(ccxt_symbol, timeframe=timeframe, limit=min(remaining, max_per_request))
            if batch:
                all_candles = batch
                remaining -= len(batch)

            # Fetch older batches
            while remaining > 0 and all_candles:
                oldest_ts = all_candles[0][0]
                # Calculate 'since' to get candles before the oldest we have
                tf_ms = exchange.parse_timeframe(timeframe) * 1000
                since = oldest_ts - (min(remaining, max_per_request) * tf_ms)
                batch = exchange.fetch_ohlcv(
                    ccxt_symbol, timeframe=timeframe, since=since, limit=min(remaining, max_per_request),
                )
                if not batch or batch[-1][0] >= oldest_ts:
                    break  # No more data or overlap
                # Filter out any overlap
                batch = [c for c in batch if c[0] < oldest_ts]
                all_candles = batch + all_candles
                remaining -= len(batch)

            ohlcv = all_candles

        # Clean up exchange connection (sync client may not have close())
        if hasattr(exchange, "close"):
            exchange.close()

        if not ohlcv:
            msg = f"No candle data returned for {ccxt_symbol}"
            raise RuntimeError(msg)

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        logger.info(
            "candles_fetched",
            symbol=symbol,
            count=len(df),
            from_ts=str(df["timestamp"].iloc[0]),
            to_ts=str(df["timestamp"].iloc[-1]),
        )

        return df

    except Exception as e:
        logger.error("candle_fetch_failed", symbol=symbol, exchange=exchange_source, error=str(e))
        raise RuntimeError(f"Failed to fetch candles for {symbol}: {e}") from e


# Backward-compatible alias
fetch_15m_candles = fetch_candles


async def fetch_2h_candles(
    symbol: str,
    exchange_source: str = "binance",
    limit: int = 2,
) -> pd.DataFrame:
    """Fetch 2-hour OHLCV candles via CCXT.

    Args:
        symbol: Simple symbol (e.g. 'BTC').
        exchange_source: CCXT exchange ID (default: binance).
        limit: Number of candles to fetch (default: 2).

    Returns:
        DataFrame with columns: timestamp, open, high, low, close, volume.

    Raises:
        RuntimeError: If fetching fails.
    """
    ccxt_symbol = _map_symbol(symbol, exchange_source)

    logger.info(
        "fetching_2h_candles",
        symbol=symbol,
        ccxt_symbol=ccxt_symbol,
        exchange=exchange_source,
        limit=limit,
    )

    try:
        exchange_cls = getattr(ccxt, exchange_source)
        exchange = exchange_cls({"enableRateLimit": True})

        ohlcv = exchange.fetch_ohlcv(ccxt_symbol, timeframe="2h", limit=limit)

        # Clean up exchange connection (sync client may not have close())
        if hasattr(exchange, "close"):
            exchange.close()

        if not ohlcv:
            msg = f"No 2H candle data returned for {ccxt_symbol}"
            raise RuntimeError(msg)

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        logger.info(
            "2h_candles_fetched",
            symbol=symbol,
            count=len(df),
            from_ts=str(df["timestamp"].iloc[0]),
            to_ts=str(df["timestamp"].iloc[-1]),
        )

        return df

    except Exception as e:
        logger.error("2h_candle_fetch_failed", symbol=symbol, exchange=exchange_source, error=str(e))
        raise RuntimeError(f"Failed to fetch 2H candles for {symbol}: {e}") from e


def get_counter_trend_sl(
    df_2h: pd.DataFrame,
    entry_price: float,
    is_long: bool,
    sl_buffer_pct: float = 0.1,
) -> float | None:
    """Get structural SL from the last 2 candles (current forming + previous completed).

    For longs: lowest low of the 2 candles minus buffer.
    For shorts: highest high of the 2 candles plus buffer.
    The buffer avoids getting wicked out on the exact pip.

    Args:
        df_2h: DataFrame with 2H candles (from fetch_2h_candles).
        entry_price: Current entry/fill price.
        is_long: True for long trades, False for short.
        sl_buffer_pct: Buffer as % from candle level (default 0.1 = 0.1%).

    Returns:
        Structural SL price, or None if insufficient data or invalid level.
    """
    if len(df_2h) < 2:
        logger.warning("insufficient_2h_candles", count=len(df_2h))
        return None

    # iloc[-1] = current forming candle, iloc[-2] = last completed candle
    current_candle = df_2h.iloc[-1]
    previous_candle = df_2h.iloc[-2]

    buffer_mult = sl_buffer_pct / 100  # 0.1 → 0.001

    if is_long:
        raw_sl = min(float(current_candle["low"]), float(previous_candle["low"]))
        sl = round(raw_sl * (1 - buffer_mult), 2)
        logger.info(
            "counter_trend_sl_calculated",
            direction="long",
            current_2h_low=round(float(current_candle["low"]), 2),
            previous_2h_low=round(float(previous_candle["low"]), 2),
            raw_sl=round(raw_sl, 2),
            sl_with_buffer=sl,
            entry_price=entry_price,
            valid=sl < entry_price,
        )
        return sl if sl < entry_price else None
    else:
        raw_sl = max(float(current_candle["high"]), float(previous_candle["high"]))
        sl = round(raw_sl * (1 + buffer_mult), 2)
        logger.info(
            "counter_trend_sl_calculated",
            direction="short",
            current_2h_high=round(float(current_candle["high"]), 2),
            previous_2h_high=round(float(previous_candle["high"]), 2),
            raw_sl=round(raw_sl, 2),
            sl_with_buffer=sl,
            entry_price=entry_price,
            valid=sl > entry_price,
        )
        return sl if sl > entry_price else None


# ---------------------------------------------------------------------------
# Trend Filter Calculation (Sander's Pandas math)
# ---------------------------------------------------------------------------


@dataclass
class TrendData:
    """Result of trend filter calculation."""

    ema_scaled: float
    delta: float
    ema_50: float
    ema_200: float
    ema_fast: float
    slope_rising: bool
    ema_scaled_prev: float = 0.0
    delta_prev: float = 0.0
    in_delta_channel: bool = False
    is_fast_market: bool = False
    atr_regime_pct: float = 0.0
    slope_gate_used: str = "ema_slope"
    ema_slope_history: list[float] = field(default_factory=list)
    delta_slope_history: list[float] = field(default_factory=list)


@dataclass
class FilterResult:
    """Result of pre-execution filter checks (Phase 2)."""

    passed: bool
    should_wait: bool = False
    size_multiplier: float = 1.0
    rejection_reason: str | None = None


def calculate_trend_filters(
    df: pd.DataFrame,
    *,
    ema_macro_span: int = 120,
    slope_smooth_bars: int = 7,
    delta_smooth_bars: int = 3,
    scale_window: int = 500,
    slope_method: str = "ema_slope",
    use_delta_ema: bool = False,
    delta_ntz: float = 5.0,
    use_atr_regime: bool = False,
    atr_length: int = 14,
    atr_ema_length: int = 20,
    atr_fast_threshold: float = 20.0,
    atr_df: pd.DataFrame | None = None,
) -> TrendData:
    """Translate Sander's Pine Script MTF math into Pandas.

    Computes:
    1. EMA of close prices (configurable span)
    2. Slope = EMA - EMA[smooth_bars ago]
    3. Scaled slope = 100 * slope / rolling range of slope
    4. Slope direction depends on slope_method:
       - ema_slope: ma_df_scaled[-1] > ma_df_scaled[-2]
       - delta_ema_slope: EMA[-1] > EMA[-2] (raw single-bar delta)
    5. Delta slope = EMA-fast minus EMA-50, min-max scaled (TV parity)

    Also computes EMA-50 and EMA-200 for value zone detection.

    Args:
        df: DataFrame with at least a 'close' column (500+ rows recommended).
        ema_macro_span: EMA span for macro trend (default 120).
        slope_smooth_bars: Lookback bars for slope (default 7).
        delta_smooth_bars: Lookback bars for delta momentum (default 3).
        scale_window: Rolling window for min-max normalization (default 500).
        slope_method: 'ema_slope' (smoothed lookback) or 'delta_ema_slope' (raw EMA delta).

    Returns:
        TrendData with ema_scaled, delta, ema_50, ema_200, slope_rising.
    """
    # Calculate Base EMAs
    df["ema_120"] = df["close"].ewm(span=ema_macro_span, adjust=False).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()
    df["ema_fast"] = df["close"].ewm(span=15, adjust=False).mean()

    # EMA Slope
    df["ma_df"] = df["ema_120"] - df["ema_120"].shift(slope_smooth_bars)

    # Scale the slope
    df["ma_range"] = df["ma_df"].rolling(window=scale_window, min_periods=1).max() - df["ma_df"].rolling(window=scale_window, min_periods=1).min()
    df["ma_range"] = df["ma_range"].replace(0, 1)  # Prevent division by zero
    df["ma_df_scaled"] = 100 * df["ma_df"] / df["ma_range"]

    # ATR Regime Detector (Sander's Pine: ATR Regime)
    # Determines fast vs slow market for dynamic slope gate switching
    is_fast_market = False
    atr_regime_pct_val = 0.0
    slope_gate_used = "ema_slope"

    if use_atr_regime:
        # Use separate ATR DataFrame if provided (e.g. 5m candles),
        # otherwise fall back to the main df (same timeframe)
        _atr_src = atr_df if atr_df is not None else df
        # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
        _atr_src["tr"] = pd.concat([
            _atr_src["high"] - _atr_src["low"],
            (_atr_src["high"] - _atr_src["close"].shift(1)).abs(),
            (_atr_src["low"] - _atr_src["close"].shift(1)).abs(),
        ], axis=1).max(axis=1)
        # Pine's ta.atr() uses RMA (Wilder's smoothing) = ewm(alpha=1/length)
        _atr_src["atr"] = _atr_src["tr"].ewm(alpha=1 / atr_length, adjust=False).mean()
        # ATR baseline uses standard EMA: ta.ema(atr, atrEmaLength)
        _atr_src["atr_ema"] = _atr_src["atr"].ewm(span=atr_ema_length, adjust=False).mean()
        # Regime % = how far ATR is above/below its baseline
        _atr_src["atr_regime_pct"] = 100.0 * (_atr_src["atr"] - _atr_src["atr_ema"]) / _atr_src["atr_ema"]

        atr_regime_pct_val = float(_atr_src["atr_regime_pct"].iloc[-1])
        is_fast_market = atr_regime_pct_val > atr_fast_threshold

        # Warn if ATR regime is on but delta EMA is off (delta needed for fast market gate)
        if not use_delta_ema:
            logger.warning(
                "atr_regime_missing_delta",
                msg="use_atr_regime=true but use_delta_ema=false — fast market gate will use EMA slope fallback",
            )

        logger.info(
            "atr_regime_detected",
            atr=round(float(_atr_src["atr"].iloc[-1]), 4),
            atr_ema=round(float(_atr_src["atr_ema"].iloc[-1]), 4),
            atr_regime_pct=round(atr_regime_pct_val, 4),
            threshold=atr_fast_threshold,
            is_fast_market=is_fast_market,
            atr_timeframe="separate" if atr_df is not None else "same",
        )

    # Slope direction check — dynamic based on ATR regime or static slope_method
    if use_atr_regime:
        if is_fast_market and use_delta_ema:
            # Fast market: use delta EMA slope (more responsive)
            # delta_scaled is computed below, so we defer slope_rising to after delta calc
            slope_gate_used = "delta_ema_slope"
            is_slope_rising = None  # Placeholder — set after delta calculation
        else:
            # Slow market (or delta disabled): use EMA slope (smoother)
            slope_gate_used = "ema_slope"
            is_slope_rising = bool(df["ma_df_scaled"].iloc[-1] > df["ma_df_scaled"].iloc[-2])
    elif slope_method == "delta_ema_slope":
        # Static mode: raw single-bar EMA delta
        ema_delta = float(df["ema_120"].iloc[-1]) - float(df["ema_120"].iloc[-2])
        is_slope_rising = ema_delta > 0
        slope_gate_used = "delta_ema_slope"
        logger.info(
            "delta_ema_slope_check",
            ema_current=round(float(df["ema_120"].iloc[-1]), 4),
            ema_previous=round(float(df["ema_120"].iloc[-2]), 4),
            ema_delta=round(ema_delta, 4),
            is_slope_rising=is_slope_rising,
        )
    else:
        # Static mode: scaled slope comparison
        is_slope_rising = bool(df["ma_df_scaled"].iloc[-1] > df["ma_df_scaled"].iloc[-2])
        slope_gate_used = "ema_slope"

    # Delta Slope — slope-of-the-slope, then min-max scaled (TV parity)
    # Pine: deltaMaDF = maDf - maDf[deltaSmoothBars]
    # Pine: fDeltaMaDf = 100 * deltaMaDF / (highest(deltaMaDF,500) - lowest(deltaMaDF,500))
    if use_delta_ema:
        df["delta_ma_df"] = df["ma_df_scaled"] - df["ma_df_scaled"].shift(delta_smooth_bars)
        d_max = df["delta_ma_df"].rolling(window=scale_window, min_periods=1).max()
        d_min = df["delta_ma_df"].rolling(window=scale_window, min_periods=1).min()
        d_range = (d_max - d_min).replace(0, 1)  # Prevent division by zero
        df["delta_scaled"] = 100 * df["delta_ma_df"] / d_range
        # NTZ flag — delta is inside neutral/dead zone (Pine: deltaNTZ)
        df["in_delta_channel"] = df["delta_scaled"].abs() < delta_ntz
    else:
        df["delta_ma_df"] = 0.0
        df["delta_scaled"] = 0.0
        df["in_delta_channel"] = False

    # Deferred slope_rising for fast market mode (needed delta_scaled computed first)
    if is_slope_rising is None and use_atr_regime and is_fast_market and use_delta_ema:
        is_slope_rising = bool(float(df["delta_scaled"].iloc[-1]) > 0)

    ema_scaled = float(df["ma_df_scaled"].iloc[-1])
    ema_scaled_prev = float(df["ma_df_scaled"].iloc[-2])
    delta = float(df["delta_scaled"].iloc[-1])
    delta_prev = float(df["delta_scaled"].iloc[-2])
    ema_50 = float(df["ema_50"].iloc[-1])
    ema_200 = float(df["ema_200"].iloc[-1])
    ema_fast = float(df["ema_fast"].iloc[-1])
    ntz_flag = bool(df["in_delta_channel"].iloc[-1])

    # Last 5 values for slope history (T-4 through T, oldest first)
    ema_slope_hist = [round(float(v), 4) for v in df["ma_df_scaled"].iloc[-5:]]
    delta_slope_hist = [round(float(v), 4) for v in df["delta_scaled"].iloc[-5:]]

    logger.info(
        "trend_calculated",
        slope_method=slope_method,
        slope_gate_used=slope_gate_used,
        slope_T=round(ema_scaled, 4),
        slope_T1=round(ema_scaled_prev, 4),
        slope_change=round(ema_scaled - ema_scaled_prev, 4),
        delta_T=round(delta, 4),
        delta_T1=round(delta_prev, 4),
        slope_rising=is_slope_rising,
        in_delta_channel=ntz_flag,
        is_fast_market=is_fast_market,
        atr_regime_pct=round(atr_regime_pct_val, 4),
        ema_50=round(ema_50, 2),
        ema_200=round(ema_200, 2),
        ema_fast=round(ema_fast, 2),
    )

    return TrendData(
        ema_scaled=ema_scaled,
        delta=delta,
        ema_50=ema_50,
        ema_200=ema_200,
        ema_fast=ema_fast,
        slope_rising=is_slope_rising,
        ema_scaled_prev=ema_scaled_prev,
        delta_prev=delta_prev,
        in_delta_channel=ntz_flag,
        is_fast_market=is_fast_market,
        atr_regime_pct=round(atr_regime_pct_val, 4),
        slope_gate_used=slope_gate_used,
        ema_slope_history=ema_slope_hist,
        delta_slope_history=delta_slope_hist,
    )


# ---------------------------------------------------------------------------
# Trend Alignment
# ---------------------------------------------------------------------------


def is_slope_aligned(action: str, trend: TrendData) -> bool:
    """Hard gate: micro momentum must hook in trade direction.

    This is non-negotiable — slope must be rising for longs,
    falling for shorts. Never buy a falling knife.

    Args:
        action: Trade action string ('entry_long' or 'entry_short').
        trend: TrendData from calculate_trend_filters().

    Returns:
        True if slope direction supports the trade.
    """
    if "long" in action:
        aligned = trend.slope_rising
    elif "short" in action:
        aligned = not trend.slope_rising
    else:
        aligned = False

    logger.info(
        "slope_alignment_check",
        action=action,
        ema_scaled=round(trend.ema_scaled, 4),
        slope_rising=trend.slope_rising,
        slope_aligned=aligned,
    )
    return aligned


def is_macro_aligned(action: str, trend: TrendData) -> bool:
    """Soft gate: macro EMA slope above/below zero.

    Longs require EMA50 > EMA200 (bullish macro trend).
    Shorts require EMA50 < EMA200 (bearish macro trend).
    This check can be overridden by the counter-trend half-size setting.

    Args:
        action: Trade action string ('entry_long' or 'entry_short').
        trend: TrendData from calculate_trend_filters().

    Returns:
        True if macro trend supports the trade direction.
    """
    if "long" in action:
        aligned = trend.ema_50 > trend.ema_200
    elif "short" in action:
        aligned = trend.ema_50 < trend.ema_200
    else:
        aligned = False

    logger.info(
        "macro_alignment_check",
        action=action,
        ema_scaled=round(trend.ema_scaled, 4),
        macro_aligned=aligned,
    )
    return aligned


# ---------------------------------------------------------------------------
# Value Zone Sizing
# ---------------------------------------------------------------------------


def value_zone_multiplier(
    df: pd.DataFrame,
    is_long: bool,
    vz_memory_bars: int = 20,
) -> float:
    """Determine position size multiplier based on value zone memory.

    Checks the last ``lookback`` candles (pullback phase) to see if price
    was in the value zone at any point. This prevents unfair half-sizing
    when price briefly pokes outside the zone on the entry candle.

    Value zone = price between EMA-50 and EMA-200 (regardless of which is higher).
    - Any candle in lookback was in zone: 1.0 (full size)
    - No candle in lookback was in zone: 0.5 (half size)

    Args:
        df: DataFrame with 'close', 'ema_50', 'ema_200' columns
            (populated by calculate_trend_filters).
        is_long: True for long trades.
        vz_memory_bars: Number of recent candles to check (default 20).

    Returns:
        1.0 for value zone, 0.5 for momentum zone.
    """
    tail = df.tail(vz_memory_bars)

    # Value zone = price between the two EMAs, regardless of which is higher
    lower = tail[["ema_50", "ema_200"]].min(axis=1)
    upper = tail[["ema_50", "ema_200"]].max(axis=1)
    in_zone = bool(((lower < tail["close"]) & (tail["close"] < upper)).any())

    multiplier = 1.0 if in_zone else 0.5

    logger.info(
        "value_zone_check",
        is_long=is_long,
        lookback=vz_memory_bars,
        in_value_zone=in_zone,
        multiplier=multiplier,
    )

    return multiplier


# ---------------------------------------------------------------------------
# Pre-Execution Filters (Phase 2)
# ---------------------------------------------------------------------------


def check_pre_execution_filters(
    trend: TrendData,
    current_price: float,
    settings: Settings,
    *,
    is_long: bool = True,
    is_counter_trend: bool = False,
    df_2h: pd.DataFrame | None = None,
) -> FilterResult:
    """Run volatility, chop, and anti-chase filters before trade execution.

    Filter order:
    1. Overextended: gap between EMA-50 and EMA-200 > max_ema_gap_pct → wait (trend trades only)
    2. Chop: gap < min_ema_gap_pct → wait (trend trades only)
    3. Anti-chase: price too far from last 2H structural level → reject or half_size.
       Uses last 2H low for longs, last 2H high for shorts.
       Falls back to EMA-50 if no 2H data available.

    Args:
        trend: TrendData from calculate_trend_filters().
        current_price: Current mark price.
        settings: Application settings with filter thresholds.
        is_long: True for long trades, False for shorts.
        is_counter_trend: If True, skip gap filters (A and B).
        df_2h: Optional 2H candle DataFrame for structural chase detection.

    Returns:
        FilterResult indicating pass/fail and any size adjustments.
    """
    from app.models import ChaseAction

    gap_pct = (abs(trend.ema_50 - trend.ema_200) / trend.ema_200) * 100

    if not is_counter_trend:
        # A. Overextended filter
        if gap_pct > settings.max_ema_gap_pct:
            logger.info(
                "filter_overextended",
                gap_pct=round(gap_pct, 2),
                max=settings.max_ema_gap_pct,
            )
            return FilterResult(
                passed=False,
                should_wait=True,
                rejection_reason=f"overextended: gap {gap_pct:.2f}% > {settings.max_ema_gap_pct}%",
            )

        # B. Chop filter
        if gap_pct < settings.min_ema_gap_pct:
            logger.info(
                "filter_chop",
                gap_pct=round(gap_pct, 2),
                min=settings.min_ema_gap_pct,
            )
            return FilterResult(
                passed=False,
                should_wait=True,
                rejection_reason=f"chop: gap {gap_pct:.2f}% < {settings.min_ema_gap_pct}%",
            )

    # C. Anti-chase filter — use 2H structural level if available
    if df_2h is not None and len(df_2h) >= 2:
        if is_long:
            structural_level = min(float(df_2h.iloc[-1]["low"]), float(df_2h.iloc[-2]["low"]))
            chase_pct = (current_price - structural_level) / structural_level * 100
        else:
            structural_level = max(float(df_2h.iloc[-1]["high"]), float(df_2h.iloc[-2]["high"]))
            chase_pct = (structural_level - current_price) / structural_level * 100
        logger.info(
            "anti_chase_structural",
            is_long=is_long,
            structural_level=round(structural_level, 2),
            current_price=current_price,
            chase_pct=round(chase_pct, 2),
        )
    else:
        # Fallback to EMA-50 based chase if no 2H data
        if is_long:
            chase_pct = (current_price - trend.ema_50) / trend.ema_50 * 100
        else:
            chase_pct = (trend.ema_50 - current_price) / trend.ema_50 * 100
        logger.info(
            "anti_chase_ema50_fallback",
            is_long=is_long,
            ema_50=round(trend.ema_50, 2),
            current_price=current_price,
            chase_pct=round(chase_pct, 2),
        )

    if chase_pct > settings.max_chase_pct:
        if settings.chase_action == ChaseAction.REJECT:
            logger.info(
                "filter_chase_reject",
                chase_pct=round(chase_pct, 2),
                max=settings.max_chase_pct,
            )
            return FilterResult(
                passed=False,
                should_wait=False,
                rejection_reason=(
                    f"chase: price {chase_pct:.2f}% from structural level > {settings.max_chase_pct}%"
                ),
            )
        # HALF_SIZE
        logger.info(
            "filter_chase_half_size",
            chase_pct=round(chase_pct, 2),
            max=settings.max_chase_pct,
        )
        return FilterResult(passed=True, size_multiplier=0.5)

    # All filters passed
    return FilterResult(passed=True)

# ---------------------------------------------------------------------------
# Dynamic Exit Price Calculation (Phase 7)
# ---------------------------------------------------------------------------


@dataclass
class ExitPrices:
    """Computed exit prices for a trade."""

    sl_price: float
    breakeven_price: float
    tp1_price: float
    trail_activation_price: float
    trail_lock_price: float
    tp2_price: float


def calculate_exit_prices(
    entry_price: float,
    is_long: bool,
    settings: Settings,
) -> ExitPrices:
    """Compute all exit prices from entry price and config percentages.

    All prices are calculated as offsets from the entry price.
    For LONG trades, targets are above entry and SL is below.
    For SHORT trades, targets are below entry and SL is above.

    Args:
        entry_price: Fill price of the entry order.
        is_long: True for long, False for short.
        settings: Application settings with threshold percentages.

    Returns:
        ExitPrices with all computed levels.
    """
    direction = 1 if is_long else -1

    sl_price = round(entry_price * (1 - (settings.trade_initial_sl_pct / 100) * direction), 2)
    breakeven_price = round(entry_price * (1 + (settings.trade_be_trigger_pct / 100) * direction), 2)
    tp1_price = round(entry_price * (1 + (settings.trade_tp1_trigger_pct / 100) * direction), 2)
    trail_activation_price = round(entry_price * (1 + (settings.trade_trail_active_pct / 100) * direction), 2)
    trail_lock_price = round(entry_price * (1 + (settings.trade_trail_lock_pct / 100) * direction), 2)
    tp2_price = round(entry_price * (1 + (settings.trade_tp2_trigger_pct / 100) * direction), 2)

    exits = ExitPrices(
        sl_price=sl_price,
        breakeven_price=breakeven_price,
        tp1_price=tp1_price,
        trail_activation_price=trail_activation_price,
        trail_lock_price=trail_lock_price,
        tp2_price=tp2_price,
    )

    logger.info(
        "exit_prices_calculated",
        entry_price=entry_price,
        is_long=is_long,
        sl=round(sl_price, 2),
        be=round(breakeven_price, 2),
        tp1=round(tp1_price, 2),
        trail_act=round(trail_activation_price, 2),
        trail_lock=round(trail_lock_price, 2),
        tp2=round(tp2_price, 2),
    )

    return exits
