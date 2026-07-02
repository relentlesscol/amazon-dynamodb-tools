"""Test for issue #144: Dynamically report if XTimeout seems insufficient.

When a table has 2 billion items and a 10,000 XMaxReadRate, at 60 min
timeout it's obvious the job won't finish. The system should warn the
user early (as part of the cost estimate) when estimated completion time
exceeds the configured timeout.

This tests server-side behavior: a function that, given table item count,
read rate, and timeout, produces a warning message or raises an error.
"""

import importlib
import pathlib
import sys
from unittest.mock import MagicMock, patch

import pytest


def _load_real_table_info():
    """Load the real table_info module with its dependencies mocked."""
    import types
    # Provide the minimal mocks that table_info needs at import time
    logger_mock = types.ModuleType('python_modules.shared.logger')
    logger_mock.log = MagicMock()
    pricing_mock = types.ModuleType('python_modules.shared.pricing')
    pricing_mock.PricingUtility = MagicMock

    saved = {}
    for mod_name in ['python_modules.shared.logger', 'python_modules.shared.pricing',
                     'shared.logger', 'shared.pricing']:
        saved[mod_name] = sys.modules.get(mod_name)

    sys.modules['python_modules.shared.logger'] = logger_mock
    sys.modules['python_modules.shared.pricing'] = pricing_mock
    sys.modules['shared.logger'] = logger_mock
    sys.modules['shared.pricing'] = pricing_mock

    table_info_path = pathlib.Path(__file__).resolve().parents[2] / "server/src/python_modules/shared/table_info.py"
    spec = importlib.util.spec_from_file_location("_real_table_info_144", str(table_info_path))
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception:
        mod = None
    finally:
        for mod_name, val in saved.items():
            if val is None:
                sys.modules.pop(mod_name, None)
            else:
                sys.modules[mod_name] = val
    return mod


_real_table_info = _load_real_table_info()


class TestTimeoutInsufficiencyDetection:
    """The system should warn when estimated scan time exceeds XTimeout."""

    @pytest.fixture(autouse=True)
    def check_function_exists(self):
        if _real_table_info is None:
            pytest.fail("Could not import table_info module")
        if not hasattr(_real_table_info, 'check_timeout_sufficiency'):
            pytest.fail(
                "table_info.check_timeout_sufficiency() does not exist — "
                "this function should estimate scan time and compare to XTimeout"
            )

    def test_warns_when_estimated_time_exceeds_timeout(self, capsys):
        """A table with 2B items at 10k read rate takes ~55 hours.
        With a 60-min timeout, the system must warn."""
        table_info = {
            'item_count': 2_000_000_000,
            'size_bytes': 400_000_000_000,  # 400 GB
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'read_pricing_category': 'std_rcu_pricing',
        }
        args = {
            'XMaxReadRate': 10000,
            'XTimeout': 60,  # 60 minutes
        }
        result = _real_table_info.check_timeout_sufficiency(table_info, args)
        assert result['will_timeout'] is True
        assert result['estimated_minutes'] > 60

    def test_no_warning_when_time_is_sufficient(self):
        """A small table with high read rate finishes well within timeout."""
        table_info = {
            'item_count': 1_000,
            'size_bytes': 100_000,  # 100 KB
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'read_pricing_category': 'std_rcu_pricing',
        }
        args = {
            'XMaxReadRate': 10000,
            'XTimeout': 60,
        }
        result = _real_table_info.check_timeout_sufficiency(table_info, args)
        assert result['will_timeout'] is False

    def test_uses_default_timeout_when_not_specified(self):
        """When XTimeout is not in args, uses the default 60 minutes."""
        table_info = {
            'item_count': 2_000_000_000,
            'size_bytes': 400_000_000_000,
            'region_name': 'us-east-1',
            'billing_mode': 'PAY_PER_REQUEST',
            'read_pricing_category': 'std_rcu_pricing',
        }
        args = {
            'XMaxReadRate': 10000,
            # No XTimeout specified
        }
        result = _real_table_info.check_timeout_sufficiency(table_info, args)
        assert result['will_timeout'] is True
