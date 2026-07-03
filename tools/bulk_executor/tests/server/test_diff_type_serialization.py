"""Failing tests for issue #142: Bulk's `diff` command has difficulty with some types.

The diff command uses `json.dumps(item, sort_keys=True)` which fails for items
containing Python `bytes` objects (DynamoDB Binary type) and DynamoDB Set types
that are represented as Python sets (which are not JSON-serializable).

The current code has a BinaryAwareEncoder that handles `bytes`, but the issue
specifically reports that items containing DynamoDB `bytes` values (returned by
the low-level Client API as `Binary` objects that inherit `bytes`) fail when
compared via `item_matches` and output via `log_diff`.

This test validates that `item_matches` and `log_diff` handle items whose values
contain `Decimal` type (used by boto3's TypeDeserializer for Number types) and
`set` types (used for SS/NS/BS in high-level Resource API).

Note: The low-level Client API returns all numbers as strings in {'N': '123'}
format and binary as bytes in {'B': b'...'} format. The BinaryAwareEncoder
already handles raw bytes. But items deserialized via TypeDeserializer can
contain Decimal and set objects. This test ensures robustness.
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


class TestDiffHandlesDecimalType:
    """item_matches must handle items containing Decimal values.

    When items are deserialized using boto3's TypeDeserializer, Number
    values become Decimal objects. json.dumps raises TypeError for Decimal
    unless a custom encoder handles it.
    """

    def test_item_matches_with_decimal_values(self):
        """Two items with Decimal number values should compare without error."""
        a = {'pk': {'S': 'k'}, 'price': {'N': Decimal('19.99')}}
        b = {'pk': {'S': 'k'}, 'price': {'N': Decimal('19.99')}}
        # Should not raise TypeError: Object of type Decimal is not JSON serializable
        assert diff_module.item_matches(a, b)

    def test_item_matches_different_decimals(self):
        """Different Decimal values should not match."""
        a = {'pk': {'S': 'k'}, 'price': {'N': Decimal('19.99')}}
        b = {'pk': {'S': 'k'}, 'price': {'N': Decimal('29.99')}}
        assert not diff_module.item_matches(a, b)

    def test_log_diff_with_decimal_value(self):
        """log_diff should handle items containing Decimal without error."""
        item = {'pk': {'S': 'a'}, 'amount': {'N': Decimal('100.50')}}
        stream = MagicMock()
        stream.head.return_value = item
        stream.pk = 'pk'
        stream.sk = None
        # Should not raise TypeError
        result = diff_module.log_diff('-', stream, False)
        assert result.startswith('-')
        assert '100.50' in result or '100.5' in result


class TestDiffHandlesSetTypes:
    """item_matches must handle items containing Python set objects.

    DynamoDB Set types (SS, NS, BS) can be represented as Python `set`
    or `frozenset` when deserialized. json.dumps raises TypeError for sets.
    """

    def test_item_matches_with_set_values(self):
        """Items with Python set values should compare without error."""
        a = {'pk': {'S': 'k'}, 'tags': {'SS': set(['red', 'blue', 'green'])}}
        b = {'pk': {'S': 'k'}, 'tags': {'SS': set(['red', 'blue', 'green'])}}
        # Should not raise TypeError: Object of type set is not JSON serializable
        assert diff_module.item_matches(a, b)

    def test_item_matches_different_sets(self):
        """Different set values should not match."""
        a = {'pk': {'S': 'k'}, 'tags': {'SS': set(['red', 'blue'])}}
        b = {'pk': {'S': 'k'}, 'tags': {'SS': set(['red', 'green'])}}
        assert not diff_module.item_matches(a, b)

    def test_log_diff_with_set_value(self):
        """log_diff should handle items containing sets without error."""
        item = {'pk': {'S': 'a'}, 'nums': {'NS': set(['1', '2', '3'])}}
        stream = MagicMock()
        stream.head.return_value = item
        stream.pk = 'pk'
        stream.sk = None
        # Should not raise TypeError
        result = diff_module.log_diff('-', stream, False)
        assert result.startswith('-')
