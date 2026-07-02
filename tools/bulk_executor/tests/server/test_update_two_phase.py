"""Unit tests for issue #87: two-phase scan→parallel write architecture.

The current `update` command has each worker scan its segment AND write
updates inline. When a single partition key dominates the data (hot key),
the worker assigned that segment becomes the long pole — limited to ~200
UpdateItem calls/sec due to serial round-trip latency.

Issue #87 introduces a two-phase mode:
  Phase 1 (scan):  Workers scan segments and RETURN update commands
                   (the kwargs for update_item) without executing writes.
  Phase 2 (write): Commands are repartitioned (scattered) across all
                   workers via Spark, then each worker executes its share
                   of writes in parallel.

The observable behavior change: writes are spread across ALL workers
regardless of which segment the item was scanned from, achieving N×
throughput instead of being bottlenecked on one worker.

These tests exercise the server-side `update` module's `run()` function
to verify that two-phase mode:
  1. Performs a scan phase that collects commands WITHOUT calling update_item
  2. Repartitions the collected commands across workers
  3. Executes writes in a separate parallel phase
  4. Reports correct totals combining both phases
"""

import sys
from unittest.mock import MagicMock, patch, call

import pytest

from python_modules import update as update_module

# Ensure shared error helpers are available (they come from `import *`)
update_module.get_error_message = MagicMock(side_effect=lambda e: str(e))
update_module.get_error_code = MagicMock(side_effect=lambda e: e.response['Error']['Code'])


# --- Fixtures ---------------------------------------------------------------

@pytest.fixture
def shared_table_info_mocks(monkeypatch):
    """Replace shared.table_info helpers used by update."""
    helpers = MagicMock()
    helpers.get_and_print_dynamodb_table_info = MagicMock(
        return_value={'item_count': 7_000_000, 'size_bytes': 5_000_000_000, 'region_name': 'us-east-1'}
    )
    helpers.get_and_print_table_scan_cost = MagicMock(return_value=3.50)
    helpers.get_dynamodb_throughput_configs = MagicMock(return_value={'opt': 'val'})

    monkeypatch.setattr(update_module, 'get_and_print_dynamodb_table_info',
                        helpers.get_and_print_dynamodb_table_info)
    monkeypatch.setattr(update_module, 'get_and_print_table_scan_cost',
                        helpers.get_and_print_table_scan_cost)
    monkeypatch.setattr(update_module, 'get_dynamodb_throughput_configs',
                        helpers.get_dynamodb_throughput_configs)
    return helpers


@pytest.fixture
def rate_limiter_mocks(monkeypatch):
    """Replace RateLimiterAggregator / RateLimiterSharedConfig."""
    config_cls = MagicMock(side_effect=lambda **kw: MagicMock(**kw))
    aggregator_cls = MagicMock()

    monkeypatch.setattr(update_module, 'RateLimiterSharedConfig', config_cls)
    monkeypatch.setattr(update_module, 'RateLimiterAggregator', aggregator_cls)
    return MagicMock(config=config_cls, aggregator=aggregator_cls)


@pytest.fixture
def base_args_two_phase():
    """Args that enable two-phase mode."""
    return {
        'table': 'hot-key-table',
        's3-bucket-name': 'rate-limit-bucket',
        'JOB_RUN_ID': 'jr-run-002',
        'two-phase': True,  # The flag that enables the new architecture
    }


# --- Two-Phase Architecture Tests -------------------------------------------

class TestTwoPhaseWriteDistribution:
    """Issue #87 core behavior: writes are spread across all workers,
    not bound to the scanning worker.

    The test simulates a hot-key scenario where 5 million of 7 million
    items share the same PK and land in one segment. In single-phase mode,
    that segment's worker would do all 5M writes serially. In two-phase
    mode, those writes are repartitioned across all workers.
    """

    def test_scan_phase_collects_commands_without_writing(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, base_args_two_phase
    ):
        """In two-phase mode, the scan phase should call generate() to
        produce update commands but MUST NOT call table.update_item().

        This is the critical architectural change: scanning is decoupled
        from writing.
        """
        # Track what the scan phase does
        scan_phase_update_calls = []
        scan_phase_generate_calls = []

        # Simulated items that would be found during scan
        items = [{'pk': 'HOT', 'sk': f'item-{i}'} for i in range(10)]

        def fake_generate(item):
            scan_phase_generate_calls.append(item)
            return {
                'Key': {'pk': item['pk'], 'sk': item['sk']},
                'UpdateExpression': 'SET #status = :v',
                'ExpressionAttributeNames': {'#status': 'status'},
                'ExpressionAttributeValues': {':v': 'processed'},
            }

        # Mock the module import for the generator
        mock_gen_module = MagicMock()
        mock_gen_module.generate = fake_generate
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=mock_gen_module))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        # Create a table mock that tracks update_item calls
        table_mock = MagicMock()
        table_mock.scan.return_value = {'Items': items}  # no LastEvaluatedKey → single page
        table_mock.update_item.side_effect = lambda **kw: scan_phase_update_calls.append(kw)

        # Wire up RateLimiterWorker to return our tracked table
        rl_worker = MagicMock()
        session = MagicMock()
        session.resource.return_value.Table.return_value = table_mock
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        # Create a spark context that executes the scan lambda
        # In two-phase mode, we expect TWO rdd.map() calls:
        #   1. scan phase → returns commands
        #   2. write phase → executes commands
        spark_context = MagicMock()

        # Track what functions are mapped
        mapped_fns = []

        def make_rdd_from_parallelize(*args, **kwargs):
            rdd = MagicMock()

            def capture_map(fn):
                mapped_fns.append(fn)
                result_rdd = MagicMock()
                # Execute the function for worker 0 to see what it does
                results = [fn(0)]
                result_rdd.collect.return_value = results
                # For chaining: flatMap / repartition / map
                result_rdd.flatMap = MagicMock(return_value=result_rdd)
                result_rdd.repartition = MagicMock(return_value=result_rdd)
                result_rdd.map = capture_map
                return result_rdd

            rdd.map = capture_map
            rdd.flatMap = MagicMock(return_value=rdd)
            rdd.repartition = MagicMock(return_value=rdd)
            return rdd

        spark_context.parallelize = make_rdd_from_parallelize
        spark_context.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args_two_phase)

        # ASSERTION: generate() was called for each scanned item
        assert len(scan_phase_generate_calls) == 10, (
            f"Expected generate() called 10 times during scan, got {len(scan_phase_generate_calls)}"
        )

        # CRITICAL ASSERTION: In two-phase mode, the scan worker must NOT
        # call update_item — writes happen in a separate phase after repartition
        assert len(scan_phase_update_calls) == 0, (
            f"Scan phase must NOT call update_item in two-phase mode, "
            f"but {len(scan_phase_update_calls)} calls were made. "
            f"This means writes are still coupled to the scanning worker."
        )

    def test_commands_are_repartitioned_before_write_phase(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, base_args_two_phase
    ):
        """After scan collects commands, they must be repartitioned across
        workers before the write phase executes them.

        This is what breaks the hot-key bottleneck: items from one segment
        get scattered to all workers for writing.
        """
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=lambda item: {'Key': item})))
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        # Track RDD operations
        repartition_called = []

        rl_worker = MagicMock()
        table_mock = MagicMock()
        table_mock.scan.return_value = {'Items': [{'pk': 'HOT', 'sk': '1'}]}
        session = MagicMock()
        session.resource.return_value.Table.return_value = table_mock
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        spark_context = MagicMock()

        def make_rdd(*args, **kwargs):
            rdd = MagicMock()

            def track_repartition(num_partitions=None):
                repartition_called.append(num_partitions)
                return rdd

            rdd.map = MagicMock(return_value=rdd)
            rdd.flatMap = MagicMock(return_value=rdd)
            rdd.repartition = track_repartition
            rdd.collect = MagicMock(return_value=[])
            return rdd

        spark_context.parallelize = make_rdd
        spark_context.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args_two_phase)

        # ASSERTION: repartition must be called to scatter writes
        assert len(repartition_called) > 0, (
            "Commands must be repartitioned across workers before write phase. "
            "repartition() was never called — writes are still bound to scan workers."
        )

    def test_two_phase_uses_multiple_rdd_operations(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, base_args_two_phase
    ):
        """In two-phase mode, run() must use at least two RDD map/flatMap
        operations — one for scan, one for write — with a repartition between.

        The current code uses a single rdd.map().collect() call, meaning
        scan and write are fused in one operation. Two-phase mode requires
        the pipeline: parallelize → map(scan) → flatMap → repartition → map(write) → collect.
        """
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=lambda item: {'Key': item})))

        rl_worker = MagicMock()
        table_mock = MagicMock()
        table_mock.scan.return_value = {'Items': [{'pk': 'HOT', 'sk': '1'}]}
        session = MagicMock()
        session.resource.return_value.Table.return_value = table_mock
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        spark_context = MagicMock()

        # Track the sequence of RDD operations called by run()
        rdd_ops_sequence = []

        def make_tracking_rdd(*args, **kwargs):
            rdd = MagicMock()

            def track_map(fn):
                rdd_ops_sequence.append('map')
                # Execute the fn so the code can proceed
                fn(0)
                result = MagicMock()
                result.map = track_map
                result.flatMap = track_flatMap
                result.repartition = track_repartition
                result.collect = MagicMock(return_value=[0])
                return result

            def track_flatMap(fn=None):
                rdd_ops_sequence.append('flatMap')
                result = MagicMock()
                result.map = track_map
                result.flatMap = track_flatMap
                result.repartition = track_repartition
                result.collect = MagicMock(return_value=[])
                return result

            def track_repartition(n=None):
                rdd_ops_sequence.append('repartition')
                result = MagicMock()
                result.map = track_map
                result.flatMap = track_flatMap
                result.repartition = track_repartition
                result.collect = MagicMock(return_value=[])
                return result

            rdd.map = track_map
            rdd.flatMap = track_flatMap
            rdd.repartition = track_repartition
            rdd.collect = MagicMock(return_value=[0])
            return rdd

        spark_context.parallelize = make_tracking_rdd
        spark_context.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args_two_phase)

        # ASSERTION: The RDD pipeline must have multiple map operations
        # with a repartition in between (scan → repartition → write)
        map_count = rdd_ops_sequence.count('map')
        assert map_count >= 2, (
            f"Two-phase mode requires at least 2 map operations "
            f"(scan phase + write phase), but only {map_count} map call(s) found. "
            f"RDD ops sequence: {rdd_ops_sequence}. "
            f"This means writes are still fused with scanning in a single pass."
        )
        assert 'repartition' in rdd_ops_sequence, (
            f"Two-phase mode requires repartition between scan and write phases. "
            f"RDD ops sequence: {rdd_ops_sequence}."
        )

    def test_two_phase_scan_returns_commands_as_rdd_elements(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, base_args_two_phase
    ):
        """In two-phase mode, the scan phase's map function must RETURN
        the update commands (as a list or iterable) so they become RDD
        elements for the next stage.

        Current code: _update_data() returns 0 (a scalar).
        Two-phase code: scan function returns list of command dicts.
        """
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())

        items = [{'pk': 'HOT', 'sk': '1'}, {'pk': 'HOT', 'sk': '2'}]

        def fake_generate(item):
            return {'Key': item, 'UpdateExpression': 'SET #x = :y'}

        mock_gen_module = MagicMock()
        mock_gen_module.generate = fake_generate
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=mock_gen_module))

        rl_worker = MagicMock()
        table_mock = MagicMock()
        table_mock.scan.return_value = {'Items': items}
        session = MagicMock()
        session.resource.return_value.Table.return_value = table_mock
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        spark_context = MagicMock()

        # Capture what the map function returns
        scan_phase_return_values = []

        def make_rdd(*args, **kwargs):
            rdd = MagicMock()

            def capture_map(fn):
                # Execute for one worker and capture its return value
                result = fn(0)
                scan_phase_return_values.append(result)
                result_rdd = MagicMock()
                result_rdd.map = MagicMock(return_value=result_rdd)
                result_rdd.flatMap = MagicMock(return_value=result_rdd)
                result_rdd.repartition = MagicMock(return_value=result_rdd)
                result_rdd.collect = MagicMock(return_value=[result])
                return result_rdd

            rdd.map = capture_map
            rdd.flatMap = MagicMock(return_value=rdd)
            rdd.repartition = MagicMock(return_value=rdd)
            rdd.collect = MagicMock(return_value=[0])
            return rdd

        spark_context.parallelize = make_rdd
        spark_context.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))

        update_module.run(MagicMock(), spark_context, MagicMock(), base_args_two_phase)

        # ASSERTION: The scan phase must return a list of commands, not 0
        assert len(scan_phase_return_values) > 0, "scan map function was not called"
        scan_result = scan_phase_return_values[0]

        assert isinstance(scan_result, list), (
            f"In two-phase mode, scan worker must return a list of update commands "
            f"(to be repartitioned), but got {type(scan_result).__name__}: {scan_result}. "
            f"Current code returns 0 (scalar) because it writes inline."
        )
        assert len(scan_result) == 2, (
            f"Expected 2 commands returned (one per scanned item needing update), "
            f"got {len(scan_result)}."
        )
        # Each command should be a dict with at least a 'Key'
        for cmd in scan_result:
            assert isinstance(cmd, dict) and 'Key' in cmd, (
                f"Each command must be a dict with 'Key', got: {cmd}"
            )


class TestTwoPhaseFlagBehavior:
    """The two-phase mode must be opt-in via a flag. When not set,
    the original single-phase behavior remains unchanged.
    """

    def test_two_phase_flag_is_recognized_by_run(
        self, monkeypatch, shared_table_info_mocks, rate_limiter_mocks, base_args_two_phase
    ):
        """run() must read the 'two-phase' flag from parsed_args and branch
        into the two-phase code path.

        The current code ignores 'two-phase' entirely — it doesn't read it.
        This test verifies the flag is consumed and changes behavior.
        """
        monkeypatch.setattr(update_module, 'print_dynamodb_table_info', MagicMock())
        monkeypatch.setattr(update_module.importlib, 'import_module',
                            MagicMock(return_value=MagicMock(generate=lambda item: {'Key': item})))

        rl_worker = MagicMock()
        table_mock = MagicMock()
        table_mock.scan.return_value = {'Items': [{'pk': 'A', 'sk': '1'}]}
        session = MagicMock()
        session.resource.return_value.Table.return_value = table_mock
        rl_worker.get_session.return_value = session
        monkeypatch.setattr(update_module, 'RateLimiterWorker', MagicMock(return_value=rl_worker))

        spark_context = MagicMock()

        # Track map calls to distinguish code paths
        map_call_count = [0]

        def make_rdd(*args, **kwargs):
            rdd = MagicMock()

            def count_map(fn):
                map_call_count[0] += 1
                fn(0)  # execute it
                result = MagicMock()
                result.map = count_map
                result.flatMap = count_map
                result.repartition = MagicMock(return_value=result)
                result.collect = MagicMock(return_value=[0])
                return result

            rdd.map = count_map
            rdd.flatMap = count_map
            rdd.repartition = MagicMock(return_value=rdd)
            rdd.collect = MagicMock(return_value=[0])
            return rdd

        spark_context.parallelize = make_rdd
        spark_context.accumulator = MagicMock(side_effect=lambda init, *_: MagicMock(value=init))

        # Run with two-phase=True
        map_call_count[0] = 0
        update_module.run(MagicMock(), spark_context, MagicMock(), base_args_two_phase)
        two_phase_map_count = map_call_count[0]

        # Run without two-phase flag
        map_call_count[0] = 0
        args_single = {k: v for k, v in base_args_two_phase.items() if k != 'two-phase'}
        update_module.run(MagicMock(), spark_context, MagicMock(), args_single)
        single_phase_map_count = map_call_count[0]

        # Two-phase mode MUST use more RDD operations than single-phase
        assert two_phase_map_count > single_phase_map_count, (
            f"Two-phase mode must use more RDD map operations than single-phase. "
            f"Got two-phase={two_phase_map_count}, single-phase={single_phase_map_count}. "
            f"The 'two-phase' flag is not changing the execution pipeline."
        )
