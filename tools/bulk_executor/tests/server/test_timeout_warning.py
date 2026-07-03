"""Unit tests for issue #144: warn user if XTimeout is insufficient.

When the item count and read rate make it clear the job cannot complete
within the configured timeout, the system should warn the user early
(during cost/time estimation) so they can abort or reconfigure.

Tests exercise the `check_timeout_sufficiency` function in
`python_modules.shared.table_info`, which computes estimated job
duration from (item_count, read_rate_per_second) and compares it
to the configured timeout in minutes.
"""

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Load real table_info module (same pattern as test_table_info.py)
sys.modules.pop('python_modules.shared.table_info', None)
sys.modules.pop('shared.table_info', None)

_TABLE_INFO_PATH = (
    Path(__file__).resolve().parents[2]
    / "server/src/python_modules/shared/table_info.py"
)
_spec = importlib.util.spec_from_file_location(
    "python_modules.shared.table_info", str(_TABLE_INFO_PATH)
)
table_info = importlib.util.module_from_spec(_spec)
sys.modules['python_modules.shared.table_info'] = table_info
_spec.loader.exec_module(table_info)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def mock_logger(monkeypatch):
    """Capture log.warn calls from table_info module."""
    logger = MagicMock()
    monkeypatch.setattr(table_info, 'log', logger)
    return logger


# --- Tests ------------------------------------------------------------------


class TestCheckTimeoutSufficiency:
    """check_timeout_sufficiency(item_count, read_rate, timeout_minutes)
    should warn the user when estimated job duration exceeds timeout."""

    def test_warns_when_timeout_clearly_insufficient(self, mock_logger):
        """2 billion items at 10,000 reads/sec = ~200,000 minutes.
        A 60-minute timeout is wildly insufficient — must warn."""
        table_info.check_timeout_sufficiency(
            item_count=2_000_000_000,
            read_rate=10_000,
            timeout_minutes=60,
        )

        # Assert a warning was emitted
        mock_logger.warn.assert_called()
        warning_msg = mock_logger.warn.call_args[0][0]
        assert 'timeout' in warning_msg.lower() or 'insufficient' in warning_msg.lower(), \
            f"Warning should mention timeout insufficiency, got: {warning_msg}"

    def test_no_warning_when_timeout_is_sufficient(self, mock_logger):
        """1000 items at 10,000 reads/sec = 0.1 seconds.
        A 60-minute timeout is more than enough — no warning."""
        table_info.check_timeout_sufficiency(
            item_count=1_000,
            read_rate=10_000,
            timeout_minutes=60,
        )

        mock_logger.warn.assert_not_called()

    def test_warns_when_timeout_barely_insufficient(self, mock_logger):
        """36,000,000 items at 10,000/sec = 3600 sec = 60 minutes.
        With a 59-minute timeout, it won't fit — should warn."""
        table_info.check_timeout_sufficiency(
            item_count=36_000_000,
            read_rate=10_000,
            timeout_minutes=59,
        )

        mock_logger.warn.assert_called()

    def test_no_warning_at_exact_boundary(self, mock_logger):
        """36,000,000 items at 10,000/sec = exactly 60 minutes.
        At a 60-minute timeout, it just barely fits — no warning."""
        table_info.check_timeout_sufficiency(
            item_count=36_000_000,
            read_rate=10_000,
            timeout_minutes=60,
        )

        mock_logger.warn.assert_not_called()

    def test_warning_includes_estimated_time(self, mock_logger):
        """The warning message should tell the user how long the job
        is estimated to take, so they can pick a better timeout."""
        table_info.check_timeout_sufficiency(
            item_count=60_000_000,
            read_rate=10_000,
            timeout_minutes=60,
        )

        mock_logger.warn.assert_called()
        warning_msg = mock_logger.warn.call_args[0][0]
        # 60M items / 10K per sec = 6000 sec = 100 min
        assert '100' in warning_msg, \
            f"Warning should include estimated time of ~100 minutes, got: {warning_msg}"

    def test_warning_includes_configured_timeout(self, mock_logger):
        """The warning should reference the configured timeout so the
        user understands the gap."""
        table_info.check_timeout_sufficiency(
            item_count=60_000_000,
            read_rate=10_000,
            timeout_minutes=60,
        )

        mock_logger.warn.assert_called()
        warning_msg = mock_logger.warn.call_args[0][0]
        assert '60' in warning_msg, \
            f"Warning should reference the 60-minute timeout, got: {warning_msg}"
