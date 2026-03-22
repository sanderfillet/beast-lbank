from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator

import uvicorn

from app.config import get_settings
from app.database import TradeDatabase
from app.exchange import LBankClient, LBankExchangeError
from app.logging_setup import get_logger, setup_logging
from app.monitor import TradeMonitor
from app.telegram import TelegramNotifier
from app.webhook import create_app

logger = get_logger("app.main")


def create_application():
    """Factory function that creates the configured FastAPI app."""

    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)

    logger.info(
        "bridge_starting",
        version="0.1.0",
        port=settings.webhook_port,
    )

    # Database
    database = TradeDatabase(settings.db_path)

    # Exchange client
    exchange_client = LBankClient(
        api_key=settings.lbank_api_key,
        api_secret=settings.lbank_api_secret,
        base_url=settings.lbank_base_url,
        sign_method=settings.lbank_sign_method,
    )

    try:
        exchange_client.connect()
    except LBankExchangeError as e:
        logger.error("exchange_connect_failed", error=str(e))

    # Telegram notifier
    notifier = None
    if settings.beast_telegram_token and settings.beast_telegram_chat_id:
        notifier = TelegramNotifier(
            token=settings.beast_telegram_token,
            chat_id=settings.beast_telegram_chat_id,
        )

    # Monitor
    monitor = TradeMonitor(
        database=database,
        exchange=exchange_client,
        settings=settings,
        notifier=notifier,
    )

    @contextlib.asynccontextmanager
    async def lifespan(fastapi_app) -> AsyncGenerator[None, None]:
        # Startup
        await database.init()
        logger.info("database_initialized")

        # Expire stale signals
        expired = await database.expire_stale_signals(settings.signal_stale_ttl_minutes)
        if expired:
            logger.info("boot_stale_signals_expired", count=expired)

        if settings.run_monitor_in_process:
            fastapi_app.state.monitor_task = asyncio.create_task(monitor.run())
            logger.info("monitor_loop_started")
        else:
            logger.info("monitor_not_started_separate_service")

        yield

        # Shutdown
        if settings.run_monitor_in_process:
            monitor.request_stop()
            if hasattr(fastapi_app.state, "monitor_task"):
                await fastapi_app.state.monitor_task

        logger.info("bridge_shutting_down")
        if notifier:
            await notifier.close()
        await database.close()
        logger.info("bridge_stopped")

    fastapi_app = create_app(settings, database, exchange_client, lifespan=lifespan)
    fastapi_app.state.monitor = monitor
    fastapi_app.state.notifier = notifier

    return fastapi_app


app = create_application()

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.webhook_host,
        port=settings.webhook_port,
        reload=False,
    )
