"""Failing tests for issue #144: Dynamically report if XTimeout seems insufficient.

The system has a default 60-minute timeout (XTimeout). When a job has a huge
number of items and a constrained read rate (XMaxReadRate), it may be obvious
before the job starts that it can't complete within the timeout.

The cost estimation phase already knows the item count and read rate. This test
asserts that a warning is emitted when the estimated scan time exceeds the
configured timeout.
"""

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())


def _load_real_table_info():
    """Load the REAL table_info module (not the mocked one from conftest)."""
    import types
    import logging

    # The module uses relative imports (from .logger import log, from .pricing import ...)
    # We need to provide these as package-level modules before exec
    _logger = logging.getLogger('table_info_real')
    _logger_mod = types.ModuleType('python_modules.shared.logger')
    _logger_mod.log = _logger

    _pricing_mod = types.ModuleType('python_modules.shared.pricing')
    _pricing_mod.PricingUtility = MagicMock()

    # Register the parent package so relative imports resolve
    _shared_pkg = types.ModuleType('python_modules.shared')
    _shared_pkg.__path__ = [str(Path(__file__).resolve().parents[2] / "server" / "src" / "python_modules" / "shared")]
    sys.modules.setdefault('python_modules', types.ModuleType('python_modules'))
    sys.modules.setdefault('python_modules.shared', _shared_pkg)
    sys.modules.setdefault('python_modules.shared.logger', _logger_mod)
    sys.modules.setdefault('python_modules.shared.pricing', _pricing_mod)

    table_info_path = Path(__file__).resolve().parents[2] / "server" / "src" / "python_modules" / "shared" / "table_info.py"
    spec = importlib.util.spec_from_file_location(
        "python_modules.shared.table_info",
        str(table_info_path),
        submodule_search_locations=[]
    )
    module = importlib.util.module_from_spec(spec)
    module.__package__ = 'python_modules.shared'
    spec.loader.exec_module(module)
    return module


class TestTimeoutInsufficientWarning:
    """The system should warn when XTimeout is insufficient for the estimated workload.

    Given a table with 2 billion items and an XMaxReadRate of 10,000 RCUs,
    the scan would take ~2 billion / (10000 * 4KB * 200 splits) time which
    far exceeds 60 minutes. The system should detect and warn about this.
    """

    def test_check_timeout_sufficiency_function_exists(self):
        """table_info module should have a check_timeout_sufficiency function."""
        table_info = _load_real_table_info()
        assert hasattr(table_info, 'check_timeout_sufficiency'), \
            "table_info module must have a check_timeout_sufficiency function"

    def test_warns_when_estimated_time_exceeds_timeout(self, capsys):
        """When item_count / read_rate suggests the job will exceed XTimeout,
        a warning should be printed during the cost estimation phase.
        """
        table_info = _load_real_table_info()

        # Scenario: 500GB table, 5000 RCUs max, 60 min timeout
        # 500GB / 8KB = 62,500,000 RCUs needed
        # At 5000/s: 12,500 seconds = ~208 minutes >> 60 min timeout
        table_info.check_timeout_sufficiency(
            size_bytes=500_000_000_000,
            max_read_rate=5000,
            timeout_minutes=60,
        )

        output = capsys.readouterr().out
        assert 'timeout' in output.lower() or 'warning' in output.lower(), \
            "Expected a warning about timeout insufficiency"

    def test_no_warning_when_timeout_is_sufficient(self, capsys):
        """When the estimated scan time is well within XTimeout, no warning."""
        table_info = _load_real_table_info()

        # Small table: 1GB, 40,000 RCUs, 60 min timeout
        # 1GB / 8KB = 125,000 RCUs needed
        # At 40,000/s: ~3 seconds. Well within 60 min.
        table_info.check_timeout_sufficiency(
            size_bytes=1_000_000_000,  # 1 GB
            max_read_rate=40000,
            timeout_minutes=60,
        )

        output = capsys.readouterr().out
        assert 'timeout' not in output.lower() and 'insufficient' not in output.lower(), \
            "Should NOT warn when timeout is sufficient"

    def test_warning_includes_estimated_time(self, capsys):
        """The warning message should include the estimated time for user clarity."""
        table_info = _load_real_table_info()

        table_info.check_timeout_sufficiency(
            size_bytes=500_000_000_000,  # 500 GB
            max_read_rate=5000,
            timeout_minutes=60,
        )

        output = capsys.readouterr().out
        # Should mention the estimated time or that it will exceed timeout
        assert any(word in output.lower() for word in ['minute', 'hour', 'exceed', 'insufficient']), \
            f"Warning should indicate time estimate, got: {output}"
