"""Structured JSON logging for distributed database nodes.

``get_logger`` returns a logger that emits one JSON object per line.
Sensitive fields (payload, value, token, authorization, secret) are redacted
by ``log_event`` and by the formatter so that no payload bytes or tokens
ever reach the logs.
"""

import contextlib
import json
import logging
import os
import sys
from datetime import UTC, datetime

# Field names whose values must never be logged in clear text.
REDACTED_FIELDS = {
    "payload",
    "value",
    "token",
    "authorization",
    "secret",
}

# Standard LogRecord attributes that are not user-supplied extra fields.
_STANDARD_ATTRS = frozenset(
    {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "asctime", "taskName",
    }
)


def _node_id():
    return (
        os.environ.get("NODE_ID")
        or os.environ.get("NODE")
        or os.environ.get("HOSTNAME")
        or "unknown"
    )


def _default(obj):
    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode("utf-8", "replace")
        except Exception:
            return repr(obj)
    if isinstance(obj, set):
        return sorted(obj, key=lambda x: str(x))
    try:
        return str(obj)
    except Exception:
        return repr(obj)


def _redact_value(key, value):
    if str(key).lower() in REDACTED_FIELDS:
        return "[REDACTED]"
    return value


def _sanitize(fields):
    if not isinstance(fields, dict):
        return fields
    out = {}
    for key, value in fields.items():
        if str(key).lower() in REDACTED_FIELDS:
            out[key] = "[REDACTED]"
        elif isinstance(value, dict):
            out[key] = _sanitize(value)
        else:
            out[key] = value
    return out


class JsonFormatter(logging.Formatter):
    def format(self, record):
        obj = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "node": _node_id(),
        }
        # Collect user-supplied extra fields.
        for key, val in getattr(record, "__dict__", {}).items():
            if key in _STANDARD_ATTRS or key.startswith("_"):
                continue
            if str(key).lower() in REDACTED_FIELDS:
                obj[key] = "[REDACTED]"
            elif isinstance(val, dict):
                obj[key] = _sanitize(val)
            else:
                obj[key] = val
        # Ensure sensitive fields are redacted even if nested in a "fields" dict.
        if "fields" in obj and isinstance(obj["fields"], dict):
            obj["fields"] = _sanitize(obj["fields"])
        # Redact sensitive keys anywhere at top level (already handled) and
        # make sure no raw sensitive value slips through via args.
        for key in list(obj.keys()):
            if str(key).lower() in REDACTED_FIELDS:
                obj[key] = "[REDACTED]"
        try:
            return json.dumps(obj, default=_default)
        except Exception:
            # Fall back to a minimal safe representation.
            safe = {k: ("[UNSERIALIZABLE]" if k not in ("ts", "level", "logger", "msg", "node") else v)
                    for k, v in obj.items()}
            try:
                return json.dumps(safe, default=_default)
            except Exception:
                return json.dumps({"ts": obj.get("ts"), "level": obj.get("level"),
                                   "logger": obj.get("logger"), "msg": str(obj.get("msg")),
                                   "node": obj.get("node")})


def get_logger(name):
    """Return a logger emitting JSON lines to stdout."""
    logger = logging.getLogger(name)
    # Avoid attaching duplicate handlers on repeated calls.
    if not any(isinstance(h, logging.StreamHandler) and getattr(h, "_json_handler", False)
               for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler._json_handler = True
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(logger, msg, level=logging.INFO, **fields):
    """Log a structured event, redacting sensitive field values."""
    safe_fields = _sanitize(fields)
    try:
        logger.log(level, msg, extra=safe_fields)
    except Exception:
        # Last-resort: never crash the caller because of logging.
        with contextlib.suppress(Exception):
            logger.log(level, msg)
    return None
