"""Test for issue #175: Add a max cost parameter.

Users want --XMaxEstimatedCostAllowed which halts the job before execution
if the estimated cost exceeds the threshold. This tests the server-side
cost-gate logic: given a cost estimate and a threshold, the system should
either proceed or raise a BulkExecutorError.
"""

import importlib
import pathlib
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


def _load_real_table_info():
    """Load the real table_info module with its dependencies mocked."""
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
    spec = importlib.util.spec_from_file_location("_real_table_info_175", str(table_info_path))
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


class TestMaxCostGate:
    """A cost gate should prevent execution when estimated cost exceeds max."""

    @pytest.fixture(autouse=True)
    def check_function_exists(self):
        if _real_table_info is None:
            pytest.fail("Could not import table_info module")
        if not hasattr(_real_table_info, 'enforce_max_cost'):
            pytest.fail(
                "table_info.enforce_max_cost() does not exist — "
                "this function should halt execution when estimated cost exceeds max"
            )

    def test_raises_when_cost_exceeds_max(self):
        """If estimated cost is $5.00 and max is $2.00, must abort."""
        with pytest.raises(Exception, match="[Cc]ost.*exceed"):
            _real_table_info.enforce_max_cost(5.00, 2.00)

    def test_does_not_raise_when_cost_within_limit(self):
        """If estimated cost is $1.50 and max is $5.00, should proceed."""
        _real_table_info.enforce_max_cost(1.50, 5.00)

    def test_does_not_raise_when_cost_equals_limit(self):
        """Exact match should still proceed (not strictly greater)."""
        _real_table_info.enforce_max_cost(3.00, 3.00)

    def test_no_enforcement_when_max_is_none(self):
        """When no max cost parameter is provided, no enforcement."""
        _real_table_info.enforce_max_cost(1000.00, None)

    def test_error_message_includes_both_values(self):
        """Error message should tell user both the estimate and their limit."""
        with pytest.raises(Exception) as exc_info:
            _real_table_info.enforce_max_cost(10.50, 5.00)
        msg = str(exc_info.value)
        assert '10.50' in msg or '10.5' in msg
        assert '5.00' in msg or '5.0' in msg
