"""Utility functions for the package."""
import re
import logging
from typing import Any, Tuple


def sanitize_url(url: str) -> str:
    """
    Mask API keys, passwords, and tokens in URLs for safe logging.

    Args:
        url: URL string that may contain sensitive data

    Returns:
        Sanitized URL with secrets masked

    Examples:
        >>> sanitize_url("https://api.example.com?api_key=secret123")
        'https://api.example.com?api_key=***MASKED***'
        >>> sanitize_url("sk-proj-abc123def456")
        'sk-***MASKED***'
    """
    if not isinstance(url, str):
        return str(url)

    patterns = [
        # API keys in query params
        (r'(api_key=)[^&\s]+', r'\1***MASKED***'),
        (r'(apikey=)[^&\s]+', r'\1***MASKED***'),
        (r'(key=)[^&\s]+', r'\1***MASKED***'),
        (r'(token=)[^&\s]+', r'\1***MASKED***'),
        (r'(password=)[^&\s]+', r'\1***MASKED***'),
        (r'(passwd=)[^&\s]+', r'\1***MASKED***'),
        (r'(secret=)[^&\s]+', r'\1***MASKED***'),
        # OpenAI API keys (sk-proj-, sk-)
        (r'sk-proj-[A-Za-z0-9_-]{20,}', r'sk-***MASKED***'),
        (r'sk-[A-Za-z0-9]{20,}', r'sk-***MASKED***'),
        # AWS keys
        (r'AKIA[0-9A-Z]{16}', r'AKIA***MASKED***'),
        # Generic bearer tokens
        (r'Bearer [A-Za-z0-9_\-\.]{20,}', r'Bearer ***MASKED***'),
    ]

    sanitized = url
    for pattern, replacement in patterns:
        sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)

    return sanitized


def sanitize_value(value: Any) -> Any:
    """
    Sanitize a value that might contain sensitive data.

    Args:
        value: Value to sanitize (string, dict, list, etc.)

    Returns:
        Sanitized value
    """
    if isinstance(value, str):
        return sanitize_url(value)
    elif isinstance(value, dict):
        return {k: sanitize_value(v) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        return type(value)(sanitize_value(v) for v in value)
    return value


class SanitizingFormatter(logging.Formatter):
    """
    Logging formatter that automatically sanitizes sensitive data.

    This formatter masks API keys, passwords, tokens, and other secrets
    in log messages to prevent credential leaks.

    Example:
        >>> handler = logging.StreamHandler()
        >>> handler.setFormatter(SanitizingFormatter(
        ...     '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ... ))
        >>> logger.addHandler(handler)
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with sanitization.

        Args:
            record: Log record to format

        Returns:
            Formatted and sanitized log message
        """
        # Make a copy to avoid modifying the original
        record = logging.makeLogRecord(record.__dict__)

        # Sanitize message
        if isinstance(record.msg, str):
            record.msg = sanitize_url(record.msg)

        # Sanitize args
        if record.args:
            if isinstance(record.args, dict):
                record.args = {k: sanitize_value(v) for k, v in record.args.items()}
            elif isinstance(record.args, tuple):
                record.args = tuple(sanitize_value(arg) for arg in record.args)
            else:
                record.args = sanitize_value(record.args)

        # Sanitize exception info if present
        if record.exc_text:
            record.exc_text = sanitize_url(record.exc_text)

        return super().format(record)


def setup_sanitized_logging(
    level: int = logging.INFO,
    format_string: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
) -> None:
    """
    Set up root logger with sanitizing formatter.

    Args:
        level: Logging level (default: INFO)
        format_string: Format string for log messages

    Example:
        >>> from finviz_weekly.utils import setup_sanitized_logging
        >>> setup_sanitized_logging(level=logging.DEBUG)
    """
    # Remove existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler with sanitizing formatter
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(SanitizingFormatter(format_string))

    # Add to root logger
    root_logger.addHandler(handler)
    root_logger.setLevel(level)


if __name__ == "__main__":
    # Test the sanitizer
    import doctest
    doctest.testmod()

    # Manual test
    print("Testing sanitization:")
    test_strings = [
        "https://api.example.com?api_key=secret123&data=value",
        "Bearer sk-proj-abc123def456ghi789",
        "Connection string: postgresql://user:password@host:5432/db",
        "Normal string without secrets",
    ]

    for s in test_strings:
        sanitized = sanitize_url(s)
        print(f"Original:  {s}")
        print(f"Sanitized: {sanitized}")
        print()
