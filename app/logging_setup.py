"""Structured logging configuration using structlog.

Provides two output formats:
- JSON: Machine-readable, ideal for production log aggregation
- Console: Human-readable with colors, ideal for development

Usage:
    from app.logging_setup import setup_logging, get_logger
    setup_logging(log_level="INFO", log_format="console")
    logger = get_logger("app.webhook")
    logger.info("webhook_received", symbol="BTC", action="entry_long")
"""

from __future__ import annotations

import logging
import sys

import structlog

from app.config import LogFormat


def setup_logging(log_level: str = "INFO", log_format: LogFormat = LogFormat.JSON) -> None:
    """Configure structlog and standard library logging.

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_format: Output format - 'json' for production, 'console' for development.
    """
    # Shared processors for both formats
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if log_format == LogFormat.CONSOLE:
        # Human-readable colored output for development
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer(
            colors=sys.stdout.isatty(),
        )
    else:
        # JSON output for production / log aggregation
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure standard library logging to route through structlog
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Silence noisy third-party loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a named structlog logger.

    Args:
        name: Logger name, typically module path (e.g., 'app.webhook').

    Returns:
        A bound structlog logger instance.
    """
    return structlog.get_logger(name)
