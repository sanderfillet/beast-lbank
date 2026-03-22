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
    signal_stale_ttl_minutes: int = Field(default=60)
    signal_cooldown_minutes: int = Field(default=5)

    # --- Candle Settings ---
    ccxt_exchange_source: str = Field(default="binance")
    candle_timeframe: str = Field(default="15m")
    candle_fetch_limit: int = Field(default=1500)
    atr_timeframe: str = Field(default="5m")
    atr_candle_limit: int = Field(default=1500)

    # --- EMA Settings ---
    ema_macro_span: int = Field(default=120)
    slope_smooth_bars: int = Field(default=7)
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
EOF
