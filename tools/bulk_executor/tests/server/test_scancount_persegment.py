"""Tests for per-segment output in scancount (issue #92).

The --persegment flag should report item counts per segment to surface
data skew. The implementation MUST use Spark Accumulators to collect
per-segment counts — plain Python dicts do NOT propagate from Spark
executors back to the driver due to closure serialization.

These tests verify:
1. run() accepts and respects a 'persegment' argument
2. A per-segment accumulator (list-based) is created to collect (segment, count) pairs
3. _count_data contributes its segment result to the per-segment accumulator
4. run() prints per-segment breakdown when persegment is enabled
"""

from unittest.mock import MagicMock, patch

import pytest

from python_modules import scancount as sc_module

sc_module.get_error_message = lambda e: str(e)


@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by scancount with fresh mocks."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 500, 'size_bytes': 4096, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=0.75)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'monitor': 'opts'})

    monkeypatch.setattr(sc_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(sc_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(sc_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig with mocks."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(sc_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(sc_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def base_args():
    return {
        'table': 'my-table',
        'index': None,
        'filter_expression': None,
        'expression_values': None,
        'expression_names': None,
        's3-bucket-name': 'rate-bucket',
        'JOB_RUN_ID': 'jr-001',
        'persegment': True,
        'segments': None,
    }


class TestPerSegmentAccumulatorCreation:
    """When persegment is enabled, run() must create an additional Spark
    Accumulator (list-based) to collect per-segment (segment_id, count)
    tuples from executors back to the driver.

    This is critical because plain Python dicts closed over by the lambda
    passed to rdd.foreach() are serialized per-executor and never propagate
    updates back to the driver process.
    """

    def test_persegment_creates_list_accumulator_for_segment_results(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, base_args
    ):
        """When persegment=True, run() must create a third accumulator
        (beyond total_matched and error) using ListAccumulator to collect
        per-segment results via Spark's accumulator protocol."""
        accumulator_calls = []

        def tracking_accumulator(init, *args):
            acc = MagicMock(value=init)
            accumulator_calls.append((init, args))
            return acc

        sc = MagicMock()
        sc.accumulator = MagicMock(side_effect=tracking_accumulator)
        rdd = MagicMock()
        sc.parallelize = MagicMock(return_value=rdd)
        rdd.foreach = MagicMock()
        rdd.count = MagicMock()

        monkeypatch.setattr(sc_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        sc_module.run(MagicMock(), sc, MagicMock(), base_args)

        # Must have at least 3 accumulators: total_matched(0), error([], ListAccumulator), segment_results([], ListAccumulator)
        assert len(accumulator_calls) >= 3, (
            f"Expected at least 3 accumulator() calls when persegment=True, got {len(accumulator_calls)}. "
            "Per-segment results must use a Spark Accumulator (not a plain dict) "
            "to propagate from executors back to the driver."
        )
        # The segment results accumulator should be list-based (initialized with [])
        segment_acc_call = accumulator_calls[2]
        assert segment_acc_call[0] == [], (
            "Per-segment accumulator must be initialized with [] (list-based) "
            "to collect (segment, count) tuples"
        )
        # It should use ListAccumulator as the AccumulatorParam
        assert len(segment_acc_call[1]) > 0 and isinstance(segment_acc_call[1][0], sc_module.ListAccumulator), (
            "Per-segment accumulator must use ListAccumulator as the AccumulatorParam "
            "so that per-worker lists are merged on the driver"
        )


class TestPerSegmentCountDataContribution:
    """_count_data must add its (segment_id, local_count) to the per-segment
    accumulator so that results propagate back to the driver via Spark's
    accumulator protocol."""

    def test_count_data_adds_segment_result_to_persegment_accumulator(self, monkeypatch):
        """_count_data must call segment_results_accumulator.add([(segment, count)])
        so the driver can report per-segment counts."""
        session = MagicMock()
        table = MagicMock()
        table.scan = MagicMock(return_value={'Count': 42})
        session.resource.return_value.Table.return_value = table

        rl = MagicMock()
        rl.get_session.return_value = session
        monkeypatch.setattr(sc_module, 'RateLimiterWorker', MagicMock(return_value=rl))

        total_acc = MagicMock()
        error_acc = MagicMock()
        segment_acc = MagicMock()

        # _count_data must accept a per-segment accumulator parameter
        # and add [(segment_id, local_count)] to it
        sc_module._count_data(
            {}, 'tbl', None, None, None, None,
            7, 200, total_acc, error_acc, MagicMock(),
            segment_acc  # per-segment accumulator
        )

        # Verify the segment accumulator received the result
        segment_acc.add.assert_called_once()
        added_value = segment_acc.add.call_args.args[0]
        # Should be a list containing a tuple/list of (segment_id, count)
        assert isinstance(added_value, list), "Must add a list to the accumulator (ListAccumulator protocol)"
        assert len(added_value) == 1, "One result per worker invocation"
        seg_id, count = added_value[0]
        assert seg_id == 7, "Segment ID must match the worker's segment"
        assert count == 42, "Count must match the local_count from scan"


class TestPerSegmentOutput:
    """When persegment is enabled, run() must print per-segment counts
    collected via the accumulator after all workers complete."""

    def test_persegment_prints_segment_breakdown(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, base_args, capsys
    ):
        """run() with persegment=True must print each segment's count,
        collected from the per-segment Spark Accumulator."""
        # Simulate 3 segments with known counts via accumulator values
        segment_results = [(0, 1000), (1, 900), (2, 123456)]

        acc_idx = [0]

        def mock_accumulator(init, *args):
            acc = MagicMock()
            idx = acc_idx[0]
            acc_idx[0] += 1
            if idx == 0:
                # total_matched
                acc.value = sum(c for _, c in segment_results)
            elif idx == 1:
                # error_accumulator
                acc.value = []
            elif idx == 2:
                # segment_results accumulator
                acc.value = segment_results
            return acc

        sc = MagicMock()
        sc.accumulator = MagicMock(side_effect=mock_accumulator)
        rdd = MagicMock()
        sc.parallelize = MagicMock(return_value=rdd)
        rdd.foreach = MagicMock()
        rdd.count = MagicMock()

        monkeypatch.setattr(sc_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        # Use only 3 segments for readable output
        base_args['segments'] = 3

        sc_module.run(MagicMock(), sc, MagicMock(), base_args)

        out = capsys.readouterr().out

        # Verify per-segment output is printed
        assert '0:' in out or 'Segment 0' in out or '0 :' in out, (
            f"Per-segment output must include segment 0's count. Got:\n{out}"
        )
        assert '1000' in out or '1,000' in out, (
            f"Per-segment output must include segment 0's count of 1000. Got:\n{out}"
        )
        assert '123456' in out or '123,456' in out, (
            f"Per-segment output must include segment 2's count of 123456. Got:\n{out}"
        )


class TestPerSegmentCustomSegmentCount:
    """The --segments flag should control how many parallel segments to use
    instead of the hardcoded 200."""

    def test_segments_arg_controls_parallelize_count(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, base_args
    ):
        """When segments=50 is provided, parallelize(range(50), 50) instead of 200."""
        sc = MagicMock()
        accs = [MagicMock(value=0), MagicMock(value=[]), MagicMock(value=[])]
        sc.accumulator = MagicMock(side_effect=accs)
        rdd = MagicMock()
        sc.parallelize = MagicMock(return_value=rdd)
        rdd.foreach = MagicMock()
        rdd.count = MagicMock()

        monkeypatch.setattr(sc_module.boto3, 'Session',
                            MagicMock(return_value=MagicMock(region_name='us-east-1')))

        base_args['segments'] = 50

        sc_module.run(MagicMock(), sc, MagicMock(), base_args)

        pc_args = sc.parallelize.call_args
        assert list(pc_args.args[0]) == list(range(50)), (
            "segments=50 should parallelize over range(50)"
        )
        assert pc_args.args[1] == 50, "numSlices should match segments count"
