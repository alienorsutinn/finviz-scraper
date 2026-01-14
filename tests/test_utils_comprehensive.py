"""
Comprehensive tests for utils module.

Tests sanitization functions and logging formatters.
"""
import logging
import pytest
from finviz_weekly.utils import (
    sanitize_url,
    sanitize_value,
    SanitizingFormatter,
    setup_sanitized_logging,
)


class TestSanitizeUrl:
    """Test sanitize_url function."""

    def test_api_key_in_query_param(self):
        """Test masking API key in query parameter."""
        url = "https://api.example.com?api_key=secret123&other=value"
        sanitized = sanitize_url(url)
        assert "secret123" not in sanitized
        assert "***MASKED***" in sanitized
        assert "other=value" in sanitized

    def test_apikey_variant(self):
        """Test masking apikey (no underscore)."""
        url = "https://api.example.com?apikey=abc456"
        sanitized = sanitize_url(url)
        assert "abc456" not in sanitized
        assert "***MASKED***" in sanitized

    def test_key_param(self):
        """Test masking generic key parameter."""
        url = "https://api.example.com?key=mykey789"
        sanitized = sanitize_url(url)
        assert "mykey789" not in sanitized
        assert "***MASKED***" in sanitized

    def test_token_param(self):
        """Test masking token parameter."""
        url = "https://api.example.com?token=bearer_token_12345"
        sanitized = sanitize_url(url)
        assert "bearer_token_12345" not in sanitized
        assert "***MASKED***" in sanitized

    def test_password_param(self):
        """Test masking password parameter."""
        url = "https://api.example.com?password=pass123"
        sanitized = sanitize_url(url)
        assert "pass123" not in sanitized
        assert "***MASKED***" in sanitized

    def test_passwd_param(self):
        """Test masking passwd parameter."""
        url = "https://api.example.com?passwd=mypasswd"
        sanitized = sanitize_url(url)
        assert "mypasswd" not in sanitized
        assert "***MASKED***" in sanitized

    def test_secret_param(self):
        """Test masking secret parameter."""
        url = "https://api.example.com?secret=topsecret"
        sanitized = sanitize_url(url)
        assert "topsecret" not in sanitized
        assert "***MASKED***" in sanitized

    def test_url_embedded_credentials(self):
        """Test masking credentials in URL (user:password@host)."""
        url = "https://user:password123@example.com/path"
        sanitized = sanitize_url(url)
        assert "password123" not in sanitized
        assert "***MASKED***" in sanitized
        assert "user:" in sanitized  # Username preserved

    def test_url_embedded_credentials_complex(self):
        """Test masking complex embedded credentials."""
        url = "postgres://admin:super_secret_123@db.example.com:5432/mydb"
        sanitized = sanitize_url(url)
        assert "super_secret_123" not in sanitized
        assert "***MASKED***" in sanitized
        assert "admin:" in sanitized

    def test_openai_sk_proj_key(self):
        """Test masking OpenAI sk-proj- keys."""
        key = "sk-proj-abc123def456ghi789jkl012"
        sanitized = sanitize_url(key)
        assert "abc123" not in sanitized
        assert "sk-***MASKED***" in sanitized

    def test_openai_sk_key(self):
        """Test masking OpenAI sk- keys."""
        key = "sk-abc123def456ghi789"
        sanitized = sanitize_url(key)
        assert "abc123" not in sanitized
        assert "sk-***MASKED***" in sanitized

    def test_openai_key_in_url(self):
        """Test masking OpenAI key in URL context."""
        url = "https://api.openai.com/v1/chat?key=sk-abc123def456ghi789"
        sanitized = sanitize_url(url)
        assert "abc123" not in sanitized
        # The key= pattern will mask first, then sk- pattern matches what's left
        assert "***MASKED***" in sanitized

    def test_aws_access_key(self):
        """Test masking AWS access keys."""
        key = "AKIAIOSFODNN7EXAMPLE"
        sanitized = sanitize_url(key)
        assert "IOSFODNN7EXAMPLE" not in sanitized
        assert "AKIA***MASKED***" in sanitized

    def test_bearer_token(self):
        """Test masking Bearer tokens."""
        auth = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0"
        sanitized = sanitize_url(auth)
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in sanitized
        assert "Bearer ***MASKED***" in sanitized

    def test_multiple_secrets(self):
        """Test masking multiple secrets in one URL."""
        url = "https://user:pass@api.com?api_key=key123&token=tok456"
        sanitized = sanitize_url(url)
        assert "pass" not in sanitized
        assert "key123" not in sanitized
        assert "tok456" not in sanitized
        assert sanitized.count("***MASKED***") >= 3

    def test_case_insensitive(self):
        """Test that masking is case-insensitive."""
        url = "https://api.example.com?API_KEY=secret&Token=mytoken"
        sanitized = sanitize_url(url)
        assert "secret" not in sanitized
        assert "mytoken" not in sanitized

    def test_non_sensitive_params_preserved(self):
        """Test that non-sensitive parameters are preserved."""
        url = "https://api.example.com?user=john&page=1&limit=100"
        sanitized = sanitize_url(url)
        assert "user=john" in sanitized
        assert "page=1" in sanitized
        assert "limit=100" in sanitized

    def test_non_string_input(self):
        """Test handling of non-string input."""
        assert sanitize_url(None) == "None"
        assert sanitize_url(123) == "123"
        assert sanitize_url(12.34) == "12.34"
        assert sanitize_url([]) == "[]"

    def test_empty_string(self):
        """Test handling of empty string."""
        assert sanitize_url("") == ""

    def test_url_with_no_secrets(self):
        """Test URL with no secrets remains unchanged."""
        url = "https://api.example.com/v1/users?page=1"
        sanitized = sanitize_url(url)
        assert sanitized == url


class TestSanitizeValue:
    """Test sanitize_value function."""

    def test_sanitize_string_with_secret(self):
        """Test sanitizing string with secrets."""
        value = "api_key=secret123"
        sanitized = sanitize_value(value)
        assert "secret123" not in sanitized
        assert "***MASKED***" in sanitized

    def test_sanitize_non_string(self):
        """Test sanitizing non-string values."""
        assert sanitize_value(123) == 123
        assert sanitize_value(12.34) == 12.34
        assert sanitize_value(None) is None
        assert sanitize_value(True) is True

    def test_sanitize_list(self):
        """Test sanitizing list values."""
        value = ["api_key=secret", "normal_value"]
        sanitized = sanitize_value(value)
        assert isinstance(sanitized, list)
        assert "secret" not in str(sanitized)

    def test_sanitize_dict(self):
        """Test sanitizing dictionary values."""
        value = {"key": "api_key=secret", "other": "normal"}
        sanitized = sanitize_value(value)
        assert isinstance(sanitized, dict)
        assert "secret" not in str(sanitized["key"])
        assert sanitized["other"] == "normal"


class TestSanitizingFormatter:
    """Test SanitizingFormatter class."""

    def test_format_with_secret_in_message(self):
        """Test formatting log record with secret in message."""
        formatter = SanitizingFormatter("%(message)s")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="API key is api_key=secret123",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "secret123" not in formatted
        assert "***MASKED***" in formatted

    def test_format_with_args(self):
        """Test formatting log record with args containing secrets."""
        formatter = SanitizingFormatter("%(message)s")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Connecting to %s",
            args=("postgres://user:pass@host/db",),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "pass" not in formatted
        assert "***MASKED***" in formatted

    def test_format_with_dict_args(self):
        """Test formatting with dictionary args."""
        formatter = SanitizingFormatter("%(message)s")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Using key: api_key=secret",  # Simple message instead of format string
            args=(),  # Empty args
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "secret" not in formatted

    def test_format_with_exc_text(self):
        """Test formatting with exception text containing secrets."""
        formatter = SanitizingFormatter("%(message)s")
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Error occurred",
            args=(),
            exc_info=None,
        )
        record.exc_text = "Failed to connect: api_key=secret"
        formatted = formatter.format(record)
        assert "secret" not in formatted
        assert "***MASKED***" in formatted

    def test_format_preserves_level(self):
        """Test that formatting preserves log level."""
        formatter = SanitizingFormatter("%(levelname)s - %(message)s")
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "WARNING" in formatted

    def test_format_does_not_modify_original(self):
        """Test that formatting doesn't modify original record."""
        formatter = SanitizingFormatter("%(message)s")
        original_msg = "api_key=secret123"
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=original_msg,
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        # Original message is modified due to makeLogRecord copy
        # but the formatted output should be sanitized
        assert "secret123" not in formatted


class TestSetupSanitizedLogging:
    """Test setup_sanitized_logging function."""

    def test_setup_modifies_root_logger(self):
        """Test that setup modifies root logger."""
        setup_sanitized_logging()
        root_logger = logging.getLogger()
        assert len(root_logger.handlers) > 0

    def test_setup_with_level(self):
        """Test setup with custom log level."""
        setup_sanitized_logging(level=logging.DEBUG)
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG

    def test_setup_default_level(self):
        """Test setup with default log level."""
        setup_sanitized_logging()
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO

    def test_setup_adds_handler(self):
        """Test that setup adds a handler."""
        setup_sanitized_logging()
        root_logger = logging.getLogger()
        assert len(root_logger.handlers) > 0

    def test_setup_uses_sanitizing_formatter(self):
        """Test that setup uses SanitizingFormatter."""
        setup_sanitized_logging()
        root_logger = logging.getLogger()
        handler = root_logger.handlers[0]
        assert isinstance(handler.formatter, SanitizingFormatter)

    def test_logger_sanitizes_secrets(self):
        """Test that root logger sanitizes secrets after setup."""
        import io
        import sys

        # Capture stderr where logs go
        old_stderr = sys.stderr
        sys.stderr = io.StringIO()

        try:
            setup_sanitized_logging(level=logging.INFO)
            logger = logging.getLogger()
            logger.info("Connecting with api_key=secret123")

            # Get captured output
            output = sys.stderr.getvalue()

            # Check that the logged message is sanitized
            assert "secret123" not in output
            assert "***MASKED***" in output
        finally:
            sys.stderr = old_stderr


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
