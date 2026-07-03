"""Failing tests for issue #142: Bulk's diff command has difficulty with some types.

The diff command uses json.dumps() for item comparison. While the BinaryAwareEncoder
handles bytes objects, DynamoDB items deserialized via the Resource API (as opposed
to the low-level Client API) can contain Python Decimal and set types that are also
not JSON serializable.

When items come back from DynamoDB's Resource API, numbers are Decimal objects and
sets (SS, NS, BS) come back as Python sets. The diff module should handle ALL
Python types that DynamoDB can return without crashing.

Note: The current code uses the low-level Client API which returns strings, but
the item_matches function is also used in contexts where items may have been
deserialized differently. This test ensures robustness.
"""

import json
import sys
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault('awsglue.transforms', MagicMock())
sys.modules.setdefault('pyspark.sql', MagicMock())
sys.modules.setdefault('pyspark.sql.functions', MagicMock())

from python_modules import diff as diff_module


class TestDiffTypeHandling:
    """item_matches and log_diff should handle all DynamoDB-native Python types."""

    def test_decimal_values_in_items(self):
        """Items with Decimal values (from Resource API) should not crash json.dumps.

        DynamoDB's Resource API returns numbers as Decimal objects.
        json.dumps raises TypeError: Object of type Decimal is not JSON serializable.
        """
        item_a = {'pk': {'S': 'key1'}, 'amount': {'N': Decimal('123.45')}}
        item_b = {'pk': {'S': 'key1'}, 'amount': {'N': Decimal('123.45')}}

        # Should not raise TypeError
        result = diff_module.item_matches(item_a, item_b)
        assert result is True

    def test_different_decimal_values_detected(self):
        """Different Decimal values should be detected as non-matching."""
        item_a = {'pk': {'S': 'key1'}, 'amount': {'N': Decimal('100.00')}}
        item_b = {'pk': {'S': 'key1'}, 'amount': {'N': Decimal('200.00')}}

        result = diff_module.item_matches(item_a, item_b)
        assert result is False

    def test_set_values_in_items(self):
        """Items with Python set values (from Resource API) should not crash.

        DynamoDB's Resource API returns SS/NS/BS as Python sets.
        json.dumps raises TypeError: Object of type set is not JSON serializable.
        """
        item_a = {'pk': {'S': 'key1'}, 'tags': {'SS': {'alpha', 'beta', 'gamma'}}}
        item_b = {'pk': {'S': 'key1'}, 'tags': {'SS': {'alpha', 'beta', 'gamma'}}}

        # Should not raise TypeError
        result = diff_module.item_matches(item_a, item_b)
        assert result is True

    def test_different_set_values_detected(self):
        """Different set values should be detected as non-matching."""
        item_a = {'pk': {'S': 'key1'}, 'tags': {'SS': {'alpha', 'beta'}}}
        item_b = {'pk': {'S': 'key1'}, 'tags': {'SS': {'alpha', 'gamma'}}}

        result = diff_module.item_matches(item_a, item_b)
        assert result is False

    def test_number_set_as_python_set(self):
        """Number sets from Resource API are Python sets of Decimal."""
        item_a = {'pk': {'S': 'key1'}, 'scores': {'NS': {Decimal('1'), Decimal('2'), Decimal('3')}}}
        item_b = {'pk': {'S': 'key1'}, 'scores': {'NS': {Decimal('1'), Decimal('2'), Decimal('3')}}}

        result = diff_module.item_matches(item_a, item_b)
        assert result is True

    def test_log_diff_with_decimal(self):
        """log_diff should handle items containing Decimal values."""
        item = {'pk': {'S': 'key1'}, 'amount': {'N': Decimal('99.99')}}

        stream = MagicMock()
        stream.head.return_value = item
        stream.pk = 'pk'
        stream.sk = None
        stream.head_key.return_value = {'pk': {'S': 'key1'}}

        # Should not raise TypeError
        result = diff_module.log_diff('-', stream, concise_format=False)
        assert 'key1' in result

    def test_log_diff_with_set(self):
        """log_diff should handle items containing Python set values."""
        item = {'pk': {'S': 'key1'}, 'tags': {'SS': {'hello', 'world'}}}

        stream = MagicMock()
        stream.head.return_value = item
        stream.pk = 'pk'
        stream.sk = None
        stream.head_key.return_value = {'pk': {'S': 'key1'}}

        # Should not raise TypeError
        result = diff_module.log_diff('+', stream, concise_format=False)
        assert 'key1' in result

    def test_mixed_complex_item(self):
        """An item with bytes, Decimal, and set should all serialize cleanly."""
        item = {
            'pk': {'S': 'complex-item'},
            'binary_data': {'B': b'\xde\xad\xbe\xef'},
            'price': {'N': Decimal('42.50')},
            'tags': {'SS': {'tag1', 'tag2'}},
            'scores': {'NS': {Decimal('1'), Decimal('2')}},
        }

        # item_matches with itself should work
        result = diff_module.item_matches(item, item)
        assert result is True
