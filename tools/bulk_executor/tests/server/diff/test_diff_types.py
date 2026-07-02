"""Test for issue #142: diff command has difficulty with some types.

The diff command's item_matches() function needs to handle ALL DynamoDB
data types including Decimal (from high-level API), set (SS/NS/BS return
Python sets in some deserializers), and nested combinations thereof.

Currently BinaryAwareEncoder handles bytes -> base64, but Decimal and set
types still cause TypeError crashes during json.dumps().
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

import sys
sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from python_modules import diff as diff_module


class TestItemMatchesHandlesAllTypes:
    """item_matches must not crash on any DynamoDB type that boto3 can return."""

    def test_items_with_decimal_values_compare_correctly(self):
        """Decimal values appear when using boto3 Table resource (high-level API).
        item_matches must handle them without crashing."""
        item_a = {'pk': {'S': 'abc'}, 'price': {'N': Decimal('19.99')}}
        item_b = {'pk': {'S': 'abc'}, 'price': {'N': Decimal('19.99')}}
        # Must not raise TypeError: Object of type Decimal is not JSON serializable
        assert diff_module.item_matches(item_a, item_b) is True

    def test_items_with_different_decimal_values_detected(self):
        """Different Decimal values should be detected as non-matching."""
        item_a = {'pk': {'S': 'abc'}, 'price': {'N': Decimal('19.99')}}
        item_b = {'pk': {'S': 'abc'}, 'price': {'N': Decimal('29.99')}}
        assert diff_module.item_matches(item_a, item_b) is False

    def test_items_with_set_values_compare_correctly(self):
        """Python set objects appear for SS/NS/BS types in some deserialization paths.
        item_matches must handle them without crashing."""
        item_a = {'pk': {'S': 'abc'}, 'tags': {'SS': {'red', 'blue'}}}
        item_b = {'pk': {'S': 'abc'}, 'tags': {'SS': {'red', 'blue'}}}
        # Must not raise TypeError: Object of type set is not JSON serializable
        assert diff_module.item_matches(item_a, item_b) is True

    def test_items_with_different_sets_detected(self):
        """Different set values should be detected as non-matching."""
        item_a = {'pk': {'S': 'abc'}, 'tags': {'SS': {'red', 'blue'}}}
        item_b = {'pk': {'S': 'abc'}, 'tags': {'SS': {'red', 'green'}}}
        assert diff_module.item_matches(item_a, item_b) is False

    def test_items_with_nested_decimal_in_map(self):
        """Decimal inside a nested Map attribute."""
        item_a = {'pk': {'S': 'x'}, 'meta': {'M': {'score': {'N': Decimal('3.14')}}}}
        item_b = {'pk': {'S': 'x'}, 'meta': {'M': {'score': {'N': Decimal('3.14')}}}}
        assert diff_module.item_matches(item_a, item_b) is True

    def test_items_with_mixed_complex_types(self):
        """Item containing Decimal, bytes, and set simultaneously."""
        item_a = {
            'pk': {'S': 'multi'},
            'data': {'B': b'\xde\xad'},
            'score': {'N': Decimal('42')},
            'labels': {'SS': {'a', 'b'}},
        }
        item_b = {
            'pk': {'S': 'multi'},
            'data': {'B': b'\xde\xad'},
            'score': {'N': Decimal('42')},
            'labels': {'SS': {'a', 'b'}},
        }
        assert diff_module.item_matches(item_a, item_b) is True


class TestLogDiffHandlesAllTypes:
    """log_diff must not crash when formatting items with non-JSON-native types."""

    def test_full_format_with_decimal(self):
        """Full-format log_diff output must handle Decimal values."""
        stream = MagicMock()
        stream.pk = 'pk'
        stream.sk = None
        stream.head.return_value = {'pk': {'S': 'abc'}, 'val': {'N': Decimal('1.5')}}
        stream.head_key.return_value = {'pk': {'S': 'abc'}}
        # Must not crash
        result = diff_module.log_diff('-', stream, False)
        assert 'abc' in result

    def test_concise_format_with_decimal(self):
        """Concise-format log_diff output must handle Decimal in keys."""
        stream = MagicMock()
        stream.pk = 'pk'
        stream.sk = None
        stream.head.return_value = {'pk': {'N': Decimal('123')}}
        stream.head_key.return_value = {'pk': {'N': Decimal('123')}}
        result = diff_module.log_diff('*', stream, True)
        assert '123' in result
