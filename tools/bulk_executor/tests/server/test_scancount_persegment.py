"""Unit tests for scancount --persegment feature (issue #92).

The --persegment flag should output per-segment item counts so users can
detect data skew across parallel scan segments. The implementation MUST use
Spark Accumulators (not plain Python dicts) to propagate per-segment counts
from executors back to the driver.

These tests verify:
1. A DictAccumulator (AccumulatorParam subclass) is used for per-segment counts
2. _count_data adds its local_count to the per-segment accumulator keyed by segment id
3. run() prints per-segment output when --persegment is set
4. run() still prints only the total when --persegment is not set (backward compat)
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from python_modules import scancount as sc_module

sc_module.get_error_message = lambda e: str(e)


# --- DictAccumulator -----------------------------------------------------------


class TestDictAccumulator:
    """Issue #92 requires a DictAccumulator (AccumulatorParam subclass) that
    merges per-segment counts from executors to the driver.

    Plain Python dicts do NOT propagate from Spark executors to the driver —
    only Accumulators do. The fix must define a DictAccumulator class."""

    def test_dict_accumulator_class_exists(self):
        """The module must define a DictAccumulator for per-segment count propagation."""
        assert hasattr(sc_module, 'DictAccumulator'), (
            "scancount must define a DictAccumulator (AccumulatorParam subclass) "
            "to propagate per-segment counts from executors to driver. "
            "Plain dicts do NOT work in Spark's distributed execution model."
        )

    def test_dict_accumulator_is_accumulator_param(self):
        """DictAccumulator must subclass AccumulatorParam for Spark compatibility."""
        from pyspark import AccumulatorParam
        acc = sc_module.DictAccumulator()
        assert isinstance(acc, AccumulatorParam), (
            "DictAccumulator must inherit from AccumulatorParam so Spark "
            "can serialize and merge it across executors."
        )

    def test_dict_accumulator_zero_returns_empty_dict(self):
        """zero() must return {} — the identity for merging segment counts."""
        acc = sc_module.DictAccumulator()
        assert acc.zero({}) == {}
        assert acc.zero(None) == {}

    def test_dict_accumulator_addInPlace_merges_counts(self):
        """addInPlace must sum values for matching keys and add new keys."""
        acc = sc_module.DictAccumulator()
        v1 = {0: 100, 1: 200}
        v2 = {1: 50, 2: 300}
        result = acc.addInPlace(v1, v2)
        assert result == {0: 100, 1: 250, 2: 300}, (
            "addInPlace should sum counts for same segment ids and add new ones"
        )
        assert result is v1, "should mutate v1 in place (Spark contract)"

    def test_dict_accumulator_addInPlace_empty_right(self):
        """Merging an empty dict leaves v1 unchanged."""
        acc = sc_module.DictAccumulator()
        v1 = {3: 500}
        result = acc.addInPlace(v1, {})
        assert result == {3: 500}

    def test_dict_accumulator_addInPlace_empty_left(self):
        """Merging into an empty dict yields v2's entries."""
        acc = sc_module.DictAccumulator()
        result = acc.addInPlace({}, {7: 42})
        assert result == {7: 42}


# --- _count_data per-segment accumulator usage ---------------------------------


class TestCountDataPerSegmentAccumulator:
    """_count_data must add its local_count to the per-segment accumulator
    (keyed by segment id) when a per_segment_accumulator is provided."""

    def _run_count_data_with_persegment(self, monkeypatch, segment=5,
                                         total_segments=200, scan_count=77):
        """Helper: run _count_data with a per-segment accumulator and return it."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': scan_count})
        session.resource.return_value.Table.return_value = table

        rl = MagicMock()
        rl.get_session.return_value = session
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        total_acc = MagicMock()
        error_acc = MagicMock()
        per_segment_acc = MagicMock()

        sc_module._count_data(
            {}, 'tbl', None, None, None, None,
            segment, total_segments,
            total_acc, error_acc, MagicMock(),
            per_segment_acc,
        )

        return per_segment_acc, total_acc

    def test_per_segment_accumulator_receives_segment_count(self, monkeypatch):
        """_count_data must call per_segment_accumulator.add({segment: local_count})."""
        per_seg_acc, _ = self._run_count_data_with_persegment(
            monkeypatch, segment=5, scan_count=77
        )
        per_seg_acc.add.assert_called_once_with({5: 77})

    def test_per_segment_accumulator_different_segment_ids(self, monkeypatch):
        """Each segment id maps its own count into the accumulator."""
        per_seg_acc, _ = self._run_count_data_with_persegment(
            monkeypatch, segment=199, scan_count=12345
        )
        per_seg_acc.add.assert_called_once_with({199: 12345})

    def test_total_accumulator_still_updated(self, monkeypatch):
        """Per-segment accumulator does not replace the total accumulator."""
        _, total_acc = self._run_count_data_with_persegment(
            monkeypatch, segment=0, scan_count=42
        )
        total_acc.add.assert_called_once_with(42)


# --- run() --persegment output -------------------------------------------------


class TestRunPerSegmentOutput:
    """When parsed_args includes persegment=True, run() must:
    1. Create a per-segment accumulator using DictAccumulator
    2. Pass it to each _count_data worker
    3. Print per-segment counts after all workers finish"""

    @pytest.fixture
    def base_args(self):
        return {
            'table': 'my-table',
            'index': None,
            'filter_expression': None,
            'expression_values': None,
            'expression_names': None,
            's3-bucket-name': 'rate-bucket',
            'JOB_RUN_ID': 'jr-001',
            'persegment': True,
        }

    @pytest.fixture
    def mock_env(self, monkeypatch):
        """Set up the mocked environment for run()."""
        monkeypatch.setattr(sc_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))
        monkeypatch.setattr(sc_module, 'RateLimiterSharedConfig', MagicMock())
        monkeypatch.setattr(sc_module, 'RateLimiterAggregator', MagicMock())
        monkeypatch.setattr(sc_module, 'get_and_print_dynamodb_table_info', MagicMock(
            return_value={'item_count': 500, 'size_bytes': 4096, 'region_name': 'us-east-1'}
        ))
        monkeypatch.setattr(sc_module, 'get_and_print_table_scan_cost', MagicMock(return_value=0.75))
        monkeypatch.setattr(sc_module, 'get_dynamodb_throughput_configs', MagicMock(return_value={}))

    def test_persegment_creates_dict_accumulator(self, monkeypatch, mock_env, base_args):
        """run() must create a per-segment accumulator using DictAccumulator when persegment=True."""
        sc = MagicMock()
        # Track accumulator calls to verify DictAccumulator is used
        accumulator_calls = []

        def track_accumulator(init, *args):
            accumulator_calls.append((init, args))
            return MagicMock(value=init)

        sc.accumulator = MagicMock(side_effect=track_accumulator)
        sc.parallelize.return_value.foreach = MagicMock()
        sc.parallelize.return_value.count = MagicMock()

        sc_module.run(MagicMock(), sc, MagicMock(), base_args)

        # Should have 3 accumulators: total (int), errors (list), per-segment (dict)
        assert len(accumulator_calls) >= 3, (
            f"Expected at least 3 accumulator() calls (total, errors, per-segment), "
            f"got {len(accumulator_calls)}"
        )
        # The per-segment accumulator should be initialized with {} and DictAccumulator
        per_seg_call = accumulator_calls[2]
        assert per_seg_call[0] == {}, (
            "Per-segment accumulator should be initialized with empty dict {}"
        )
        assert isinstance(per_seg_call[1][0], sc_module.DictAccumulator), (
            "Per-segment accumulator must use DictAccumulator (not a plain dict)"
        )

    def test_persegment_prints_per_segment_counts(self, monkeypatch, mock_env, base_args, capsys):
        """run() must print per-segment counts when persegment=True."""
        sc = MagicMock()
        # Simulate per-segment results: segments 0-4 with varying counts
        per_segment_data = {0: 1000, 1: 900, 2: 1000, 3: 123456789, 4: 1000}
        total = sum(per_segment_data.values())

        accs = [
            MagicMock(value=total),              # total_matched_accumulator
            MagicMock(value=[]),                  # error_accumulator
            MagicMock(value=per_segment_data),    # per_segment_accumulator
        ]
        sc.accumulator = MagicMock(side_effect=accs)
        sc.parallelize.return_value.foreach = MagicMock()
        sc.parallelize.return_value.count = MagicMock()

        sc_module.run(MagicMock(), sc, MagicMock(), base_args)
        out = capsys.readouterr().out

        # Each segment's count must appear in the output
        assert '0:' in out or '0 :' in out or 'Segment 0' in out, (
            f"Per-segment output must include segment 0's count. Got:\n{out}"
        )
        assert '123,456,789' in out or '123456789' in out, (
            f"Per-segment output must show segment 3's large count. Got:\n{out}"
        )

    def test_no_persegment_flag_omits_per_segment_output(self, monkeypatch, mock_env, base_args, capsys):
        """Without --persegment, run() prints only the total (backward compat)."""
        base_args['persegment'] = False

        sc = MagicMock()
        accs = [MagicMock(value=5000), MagicMock(value=[])]
        sc.accumulator = MagicMock(side_effect=accs)
        sc.parallelize.return_value.foreach = MagicMock()
        sc.parallelize.return_value.count = MagicMock()

        sc_module.run(MagicMock(), sc, MagicMock(), base_args)
        out = capsys.readouterr().out

        # Should show total but NOT per-segment breakdown
        assert '5,000' in out, "Total should still print"
        # Should not have segment-numbered lines
        for i in range(200):
            assert f'{i}: ' not in out or f'Segment {i}' not in out, (
                "Per-segment output should NOT appear without --persegment"
            )

    def test_persegment_missing_defaults_to_false(self, monkeypatch, mock_env, base_args, capsys):
        """If 'persegment' key is missing from args, behave as if False."""
        del base_args['persegment']

        sc = MagicMock()
        accs = [MagicMock(value=100), MagicMock(value=[])]
        sc.accumulator = MagicMock(side_effect=accs)
        sc.parallelize.return_value.foreach = MagicMock()
        sc.parallelize.return_value.count = MagicMock()

        # Should not raise — persegment defaults to off
        sc_module.run(MagicMock(), sc, MagicMock(), base_args)
        out = capsys.readouterr().out
        assert '100' in out
