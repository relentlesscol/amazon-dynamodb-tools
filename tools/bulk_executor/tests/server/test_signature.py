"""Test for issue #98: signature verb for comparing tables across regions or time.

The signature verb computes a deterministic hash of table segments. Each
segment produces a hash, and a final hash-of-hashes gives an overall
table signature. This enables efficient cross-region comparison without
copying data.
"""

import hashlib
from unittest.mock import MagicMock

import pytest


class TestSignature:
    """The signature verb computes a deterministic hash of table contents."""

    @pytest.fixture(autouse=True)
    def import_module(self):
        try:
            from python_modules import signature as sig_module
            self.module = sig_module
        except (ImportError, ModuleNotFoundError):
            pytest.fail("python_modules.signature does not exist")

    def test_hash_segment_is_deterministic(self):
        """Same items in same order produce the same hash."""
        items = [
            {'pk': {'S': 'a'}, 'val': {'N': '1'}},
            {'pk': {'S': 'b'}, 'val': {'N': '2'}},
        ]
        hash1 = self.module.hash_items(items)
        hash2 = self.module.hash_items(items)
        assert hash1 == hash2

    def test_different_items_produce_different_hash(self):
        """Different content produces different hashes."""
        items_a = [{'pk': {'S': 'a'}, 'val': {'N': '1'}}]
        items_b = [{'pk': {'S': 'a'}, 'val': {'N': '2'}}]
        assert self.module.hash_items(items_a) != self.module.hash_items(items_b)

    def test_hash_of_hashes_combines_segments(self):
        """The final signature is a hash of all segment hashes."""
        segment_hashes = ['abc123', 'def456', 'ghi789']
        combined = self.module.combine_hashes(segment_hashes)
        # Must be a hex string (hash output)
        assert all(c in '0123456789abcdef' for c in combined)
        # Must be deterministic
        assert combined == self.module.combine_hashes(segment_hashes)

    def test_run_outputs_overall_signature(self, monkeypatch, capsys):
        """run() should print the overall table signature."""
        spark_context = MagicMock()
        # Simulate segment hashes returned from workers
        hashes_acc = MagicMock(value=['hash0', 'hash1', 'hash2'])
        err_acc = MagicMock(value=[])
        spark_context.accumulator = MagicMock(side_effect=[hashes_acc, err_acc])
        spark_context.parallelize.return_value.map.return_value.collect.return_value = ['h1', 'h2']

        monkeypatch.setattr(self.module, 'get_and_print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(self.module, 'get_and_print_table_scan_cost', MagicMock(return_value=0))
        monkeypatch.setattr(self.module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))
        monkeypatch.setattr(self.module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(self.module, 'RateLimiterAggregator', MagicMock())

        args = {
            'table': 'test-table',
            's3-bucket-name': 'bucket',
            'JOB_RUN_ID': 'run-1',
        }

        self.module.run(MagicMock(), spark_context, MagicMock(), args)
        out = capsys.readouterr().out
        # Should print a hex signature
        assert any(c in '0123456789abcdef' for c in out)
