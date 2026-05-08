from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogFormat(StrEnum):
    JSON = "json"
    CONSOLE = "console"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- LBank API ---
    lbank_api_key: str = Field(default="")
    lbank_api_secret: str = Field(default="")
    lbank_base_url: str = Field(default="https://api.lbkex.com/")
    lbank_sign_method: str = Field(default="HmacSHA256")

    # --- Webhook ---
    webhook_secret: str = Field(default="")
    webhook_host: str = Field(default="0.0.0.0")
    webhook_port: int = Field(default=8002)

    # --- Database ---
    db_path: str = Field(default="./trades.db")

    # --- Monitor ---
    monitor_poll_interval_seconds: float = Field(default=0.5)
    memory_poll_interval_ticks: int = Field(default=120)
    snapshot_interval_hours: int = Field(default=24)
    run_monitor_in_process: bool = Field(default=False)

    # --- Trading ---
    default_trade_size_usd: float = Field(default=100.0)
    partial_exit_fraction: float = Field(default=0.5)
    min_close_notional_usd: float = Field(default=90.0)
    sl_max_retries: int = Field(default=3)
    use_spot_for_longs: bool = Field(default=False)

    # --- Signal Evaluation ---
    signal_eval_delay_minutes: int = Field(default=25)
    entry_mode: str = Field(default="slope_gate")  # 'slope_gate' or 'signal_delay'

    exchange_fee_pct: float = Field(default=0.05)  # LBank taker fee 0.05%

    # Drawdown BE — move TP1 to entry+fees when price drops X% against us
    dd_be_enabled:     bool  = Field(default=False)
    dd_be_trigger_pct: float = Field(default=0.5)
    signal_stale_ttl_minutes: int = Field(default=60)
    signal_cooldown_minutes: int = Field(default=5)

    # --- Candle Settings ---
    ccxt_exchange_source: str = Field(default="binance")
    candle_timeframe: str = Field(default="15m")
    slope_timeframe: str = Field(default="5m")
    macro_timeframe: str = Field(default="15m")
    candle_fetch_limit: int = Field(default=1500)
    atr_timeframe: str = Field(default="5m")
    atr_candle_limit: int = Field(default=1500)

    # --- EMA Settings ---
    ema_macro_span: int = Field(default=120)
    slope_smooth_bars: int = Field(default=5)
    delta_smooth_bars: int = Field(default=3)
    ema_calibration_base: int = Field(default=50)
    ema_calibration_factor: float = Field(default=10.0)
    vz_memory_bars: int = Field(default=20)
    sl_buffer_pct: float = Field(default=0.5)
    use_delta_ema: bool = Field(default=True)
    slope_method: str = Field(default="delta_ema_slope")
    delta_ntz: float = Field(default=5.0)

    # --- ATR Regime ---
    use_atr_regime: bool = Field(default=True)
    atr_length: int = Field(default=14)
    atr_ema_length: int = Field(default=20)
    atr_fast_threshold: float = Field(default=20.0)

    # --- Trade Lifecycle ---
    trade_initial_sl_pct: float = Field(default=2.0)
    trade_be_trigger_pct: float = Field(default=0.6)
    trade_tp1_trigger_pct: float = Field(default=0.9)
    trade_trail_active_pct: float = Field(default=1.5)
    trade_trail_lock_pct: float = Field(default=0.9)
    trade_tp2_trigger_pct: float = Field(default=2.5)
    counter_trend_tp2_trigger_pct: float = Field(default=5.0)  # Higher TP2 for counter-trend trades

    # Path to asset.json for per-symbol overrides
    asset_settings_path: str = Field(default="asset.json")

    @property
    def asset_settings(self) -> dict[str, dict]:
        """Load per-asset settings from asset.json. Cached after first load."""
        import json, os
        path = self.asset_settings_path
        if not os.path.exists(path):
            return {}
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return {}
    counter_trend_tp2_trigger_pct: float = Field(default=5.0)  # Higher TP for counter-trend trades

    # --- Filters ---
    max_ema_gap_pct: float = Field(default=4.0)
    min_ema_gap_pct: float = Field(default=0.3)
    max_chase_pct: float = Field(default=0.5)
    chase_action: str = Field(default="half_size")
    allow_counter_trend_half_size: bool = Field(default=False)

    # --- Slope SL ---
    enable_slope_sl: bool = Field(default=True)
    slope_sl_check_interval_ticks: int = Field(default=120)
    opposite_slope_threshold: float = Field(default=-2.0)

    # --- Telegram ---
    beast_telegram_token: str = Field(default="")
    beast_telegram_chat_id: str = Field(default="")
    telegram_enabled: bool = Field(default=True)

    # --- Logging ---
    log_level: str = Field(default="INFO")
    log_format: LogFormat = Field(default=LogFormat.JSON)

    def is_exchange_configured(self) -> bool:
        """Check if LBank API credentials are configured."""
        return bool(self.lbank_api_key and self.lbank_api_secret)


@lru_cache
def get_settings() -> Settings:
    return Settings()


# ---------------------------------------------------------------------------
# Per-symbol settings proxy
# ---------------------------------------------------------------------------

class SymbolSettings:
    """Proxy that wraps global Settings and overlays per-symbol overrides.

    Usage:
        s = SymbolSettings(settings, "BTC")
        s.trade_initial_sl_pct   # returns BTC override or global default
        s.size_usd               # returns BTC-specific size or global default

    The proxy delegates every attribute access to the underlying Settings
    object unless the asset_settings dict provides an override for the symbol.
    Lookup order: symbol-specific → "default" key → global settings value.
    """

    def __init__(self, settings: Settings, symbol: str) -> None:
        object.__setattr__(self, "_settings", settings)
        # Merge: symbol-specific on top of "default" on top of nothing
        base = settings.asset_settings.get("default", {})
        override = settings.asset_settings.get(symbol.upper(), {})
        merged = {**base, **override}
        object.__setattr__(self, "_overrides", merged)

    def __getattr__(self, name: str):
        overrides = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]
        return getattr(object.__getattribute__(self, "_settings"), name)

    # size_usd is not on Settings directly — expose it with a helper
    @property
    def symbol_size_usd(self) -> float | None:
        """Return per-symbol size_usd override, or None to use signal size."""
        overrides = object.__getattribute__(self, "_overrides")
        return overrides.get("size_usd", None)

    # Counter-trend overrides (fall back to global counter_trend_tp2_trigger_pct)
    @property
    def counter_trend_sl_pct(self) -> float:
        overrides = object.__getattribute__(self, "_overrides")
        settings = object.__getattribute__(self, "_settings")
        return overrides.get("counter_trend_sl_pct", settings.trade_initial_sl_pct)

    @property
    def counter_trend_tp2_pct(self) -> float:
        overrides = object.__getattribute__(self, "_overrides")
        settings = object.__getattribute__(self, "_settings")
        return overrides.get("counter_trend_tp2_pct", settings.counter_trend_tp2_trigger_pct)
