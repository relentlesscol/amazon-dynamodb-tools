"""Unit tests for the `signature` server-side verb.

The signature verb (issue #98) scans a DynamoDB table using segmented parallel
scans, computes a deterministic hash per segment from the items in that segment,
then produces a final hash-of-hashes. This enables efficient comparison of two
tables across regions or time without copying data.

Tests exercise the server-side module's run() function end-to-end (mocking
Spark/boto3), verifying:
- Segmented scan hashing produces deterministic per-segment hashes
- The overall hash-of-hashes is printed to stdout
- Items are serialized deterministically (sorted keys) before hashing
- Output goes to S3 when --s3 flag is set, console otherwise
- Different table contents produce different hashes
- Identical table contents produce identical hashes
"""

import hashlib
import json
from unittest.mock import MagicMock, patch, call

import pytest

from python_modules import signature as sig_module

# Inject get_error_message since star-import from Mock is empty
if not hasattr(sig_module, 'get_error_message'):
    sig_module.get_error_message = lambda e: str(e)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by signature with fresh mocks."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 100, 'size_bytes': 2048, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.50)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'monitor': 'opts'})

    monkeypatch.setattr(sig_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(sig_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(sig_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig with mocks."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(sig_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(sig_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def spark_context():
    """Mock SparkContext that records parallelize() and collect()."""
    sc = MagicMock()
    rdd = MagicMock()
    sc.parallelize = MagicMock(return_value=rdd)
    return sc


@pytest.fixture
def base_args():
    return {
        'table': 'my-table',
        'splits': '4',
        's3': None,
        's3-bucket-name': 'my-bucket',
        'JOB_RUN_ID': 'jr-sig-001',
    }


# --- Core Behavior: Deterministic Hashing -----------------------------------


class TestSignatureProducesDeterministicHash:
    """The signature verb must produce a deterministic hash from table contents.
    Given the same items in the same segments, the output hash must be identical
    across invocations."""

    def test_run_outputs_overall_hash_to_stdout(self, monkeypatch, shared_table_info_mocks,
                                                  rate_limiter_mocks, spark_context, base_args, capsys):
        """run() prints a hex-digest hash string representing the table signature."""
        # Simulate 4 segments each returning a hash string
        segment_hashes = [
            hashlib.sha256(b'segment-0-data').hexdigest(),
            hashlib.sha256(b'segment-1-data').hexdigest(),
            hashlib.sha256(b'segment-2-data').hexdigest(),
            hashlib.sha256(b'segment-3-data').hexdigest(),
        ]

        # The RDD.map().collect() returns per-segment hashes
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = segment_hashes

        monkeypatch.setattr(sig_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sig_module.boto3, 'client', MagicMock())

        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        output = capsys.readouterr().out

        # The overall hash should be a hash of the concatenated segment hashes
        expected_overall = hashlib.sha256(''.join(segment_hashes).encode()).hexdigest()
        assert expected_overall in output, (
            f"Expected overall hash {expected_overall} in output, got: {output}"
        )

    def test_same_items_produce_same_hash(self, monkeypatch, shared_table_info_mocks,
                                            rate_limiter_mocks, spark_context, base_args, capsys):
        """Identical table data across two invocations produces the same signature."""
        segment_hashes = ['aaa', 'bbb', 'ccc', 'ddd']
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = segment_hashes

        monkeypatch.setattr(sig_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sig_module.boto3, 'client', MagicMock())

        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out1 = capsys.readouterr().out

        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out2 = capsys.readouterr().out

        assert out1 == out2, "Same input should produce identical output"

    def test_different_items_produce_different_hash(self, monkeypatch, shared_table_info_mocks,
                                                     rate_limiter_mocks, spark_context, base_args, capsys):
        """Different table contents must produce a different overall signature."""
        rdd = spark_context.parallelize.return_value

        rdd.map.return_value.collect.return_value = ['hash1', 'hash2', 'hash3', 'hash4']
        monkeypatch.setattr(sig_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sig_module.boto3, 'client', MagicMock())

        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out1 = capsys.readouterr().out

        rdd.map.return_value.collect.return_value = ['XXXX', 'YYYY', 'hash3', 'hash4']
        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)
        out2 = capsys.readouterr().out

        assert out1 != out2, "Different data should produce different signatures"


# --- Segment Hashing Function -----------------------------------------------


class TestHashSegment:
    """The per-segment hashing function scans items and produces a deterministic
    hash from the serialized item stream."""

    def test_hash_segment_returns_hex_digest(self, monkeypatch):
        """hash_segment() returns a hex string (SHA-256 digest)."""
        # Mock DynamoDB scan returning items
        session = MagicMock()
        client = MagicMock()
        client.scan.return_value = {
            'Items': [
                {'pk': {'S': 'item1'}, 'data': {'N': '42'}},
                {'pk': {'S': 'item2'}, 'data': {'N': '99'}},
            ]
        }
        session.client.return_value = client

        rl_worker = MagicMock()
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(sig_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        result = sig_module.hash_segment(
            table_name='test-table',
            monitor_options={},
            segment=0,
            total_segments=4,
            rate_limiter_shared_config=MagicMock()
        )

        # Should be a valid hex string (64 chars for SHA-256)
        assert len(result) == 64, f"Expected 64-char hex digest, got {len(result)}: {result}"
        assert all(c in '0123456789abcdef' for c in result)

    def test_hash_segment_deterministic_for_same_items(self, monkeypatch):
        """Same items always produce the same segment hash."""
        items = [
            {'pk': {'S': 'a'}, 'val': {'N': '1'}},
            {'pk': {'S': 'b'}, 'val': {'N': '2'}},
        ]

        session = MagicMock()
        client = MagicMock()
        client.scan.return_value = {'Items': items}
        session.client.return_value = client

        rl_worker = MagicMock()
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(sig_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        h1 = sig_module.hash_segment('t', {}, 0, 1, MagicMock())
        # Reset scan mock
        client.scan.return_value = {'Items': items}
        h2 = sig_module.hash_segment('t', {}, 0, 1, MagicMock())

        assert h1 == h2, "Same items must produce same hash"

    def test_hash_segment_different_for_different_items(self, monkeypatch):
        """Different items must produce different hashes."""
        session = MagicMock()
        client = MagicMock()
        session.client.return_value = client

        rl_worker = MagicMock()
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(sig_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        client.scan.return_value = {'Items': [{'pk': {'S': 'a'}}]}
        h1 = sig_module.hash_segment('t', {}, 0, 1, MagicMock())

        client.scan.return_value = {'Items': [{'pk': {'S': 'z'}}]}
        h2 = sig_module.hash_segment('t', {}, 0, 1, MagicMock())

        assert h1 != h2, "Different items should produce different hashes"

    def test_hash_segment_serializes_items_with_sorted_keys(self, monkeypatch):
        """Items must be serialized with sorted keys for determinism regardless
        of the order keys appear in the scan response."""
        session = MagicMock()
        client = MagicMock()
        session.client.return_value = client

        rl_worker = MagicMock()
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(sig_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        # Same item with keys in different order
        client.scan.return_value = {'Items': [{'z': {'S': '1'}, 'a': {'S': '2'}}]}
        h1 = sig_module.hash_segment('t', {}, 0, 1, MagicMock())

        client.scan.return_value = {'Items': [{'a': {'S': '2'}, 'z': {'S': '1'}}]}
        h2 = sig_module.hash_segment('t', {}, 0, 1, MagicMock())

        assert h1 == h2, "Key order in scan response must not affect hash"

    def test_hash_segment_paginates_through_all_pages(self, monkeypatch):
        """hash_segment must scan through all pages (follow LastEvaluatedKey)."""
        session = MagicMock()
        client = MagicMock()
        scan_responses = iter([
            {'Items': [{'pk': {'S': 'a'}}], 'LastEvaluatedKey': {'pk': {'S': 'a'}}},
            {'Items': [{'pk': {'S': 'b'}}], 'LastEvaluatedKey': {'pk': {'S': 'b'}}},
            {'Items': [{'pk': {'S': 'c'}}]},  # no LEK = last page
        ])
        client.scan = MagicMock(side_effect=lambda **kw: next(scan_responses))
        session.client.return_value = client

        rl_worker = MagicMock()
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(sig_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        result = sig_module.hash_segment('t', {}, 0, 4, MagicMock())

        # Should have called scan 3 times (3 pages)
        assert client.scan.call_count == 3

        # The hash should incorporate all 3 items, not just the first page
        # Verify by computing expected hash manually
        all_items = [{'pk': {'S': 'a'}}, {'pk': {'S': 'b'}}, {'pk': {'S': 'c'}}]
        hasher = hashlib.sha256()
        for item in all_items:
            hasher.update(json.dumps(item, sort_keys=True, separators=(',', ':')).encode())
        expected = hasher.hexdigest()
        assert result == expected, f"Expected hash of all pages: {expected}, got: {result}"


# --- S3 Output Mode ---------------------------------------------------------


class TestSignatureS3Output:
    """When --s3 is set, segment hashes and overall hash go to S3."""

    def test_run_writes_hashes_to_s3_when_s3_flag_set(self, monkeypatch, shared_table_info_mocks,
                                                        rate_limiter_mocks, spark_context, base_args):
        """With --s3, results are written to S3 bucket under the job ID prefix."""
        base_args['s3'] = True

        segment_hashes = ['hash0', 'hash1', 'hash2', 'hash3']
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = segment_hashes

        s3_client = MagicMock()
        monkeypatch.setattr(sig_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sig_module.boto3, 'client', MagicMock(return_value=s3_client))

        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # Should write to S3
        s3_client.put_object.assert_called()
        put_calls = s3_client.put_object.call_args_list

        # At minimum, the overall signature should be written
        bodies = [c.kwargs.get('Body', c[1].get('Body', '')) if c.kwargs else ''
                  for c in put_calls]
        buckets = [c.kwargs.get('Bucket', '') for c in put_calls]

        assert any('my-bucket' == b for b in buckets), "Should write to configured bucket"


# --- Argument Wiring ---------------------------------------------------------


class TestSignatureRunArguments:
    """run() correctly wires parsed_args to the scanning/hashing logic."""

    def test_splits_controls_number_of_segments(self, monkeypatch, shared_table_info_mocks,
                                                  rate_limiter_mocks, spark_context, base_args):
        """The 'splits' arg determines how many segments are parallelized."""
        base_args['splits'] = '8'

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = ['h'] * 8

        monkeypatch.setattr(sig_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sig_module.boto3, 'client', MagicMock())

        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        # parallelize should be called with range(8)
        pc_args = spark_context.parallelize.call_args
        assert list(pc_args.args[0]) == list(range(8))

    def test_default_splits_is_400(self, monkeypatch, shared_table_info_mocks,
                                     rate_limiter_mocks, spark_context, base_args):
        """Without explicit splits arg, defaults to 400 segments."""
        del base_args['splits']  # Use default

        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = ['h'] * 400

        monkeypatch.setattr(sig_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sig_module.boto3, 'client', MagicMock())

        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        pc_args = spark_context.parallelize.call_args
        assert list(pc_args.args[0]) == list(range(400))


# --- Per-Segment Hash Output ------------------------------------------------


class TestSignatureOutputsPerSegmentHashes:
    """The signature verb outputs individual segment hashes so users can
    identify which segment(s) differ between two tables."""

    def test_per_segment_hashes_printed_to_console(self, monkeypatch, shared_table_info_mocks,
                                                     rate_limiter_mocks, spark_context, base_args, capsys):
        """Each segment hash is printed so the user can see where differences are."""
        segment_hashes = [
            'aaaa' * 16,
            'bbbb' * 16,
            'cccc' * 16,
            'dddd' * 16,
        ]
        rdd = spark_context.parallelize.return_value
        rdd.map.return_value.collect.return_value = segment_hashes

        monkeypatch.setattr(sig_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sig_module.boto3, 'client', MagicMock())

        sig_module.run(MagicMock(), spark_context, MagicMock(), base_args)

        output = capsys.readouterr().out
        for h in segment_hashes:
            assert h in output, f"Segment hash {h} should appear in output"
