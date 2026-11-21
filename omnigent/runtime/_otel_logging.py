"""Safe bridge from Python logging records to OpenTelemetry logs."""

from __future__ import annotations

import logging
import traceback

from opentelemetry.sdk._logs import LoggingHandler

from omnigent.process_logging import redact_log_text


class RedactingOTelLoggingHandler(LoggingHandler):
    """Export severity and trace context without bypassing process-log redaction."""

    def emit(self, record: logging.LogRecord) -> None:
        message = redact_log_text(record.getMessage())
        if record.exc_info:
            exception_text = "".join(traceback.format_exception(*record.exc_info))
            message += "\n" + redact_log_text(exception_text)
        if record.stack_info:
            message += "\n" + redact_log_text(record.stack_info)

        # Construct a fresh record so OTel cannot export raw `extra` fields,
        # exception objects, or message arguments before redaction.
        safe = logging.LogRecord(
            name=record.name,
            level=record.levelno,
            pathname=record.pathname,
            lineno=record.lineno,
            msg=message,
            args=(),
            exc_info=None,
            func=record.funcName,
        )
        safe.created = record.created
        safe.msecs = record.msecs
        super().emit(safe)
