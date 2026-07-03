"""Unit tests for max cost parameter behavior (issue #175).

When `--XMaxEstimatedCostAllowed` is provided, the runner should estimate
the cost BEFORE starting a Glue job, and refuse to start (abort with a
clear message) if the estimated cost exceeds the threshold.

This tests the OBSERVABLE BEHAVIOR of BulkDynamoDbRunner.run(): that the
Glue job is NOT started when cost exceeds the limit, and an informative
message is produced.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

# Ensure client/src modules importable
sys.path.insert(0, 'client/src')

from runner import BulkDynamoDbRunner


class TestMaxCostParameter:
    """BulkDynamoDbRunner.run() must abort before starting a Glue job
    when estimated cost exceeds XMaxEstimatedCostAllowed."""

    @pytest.fixture
    def runner(self, monkeypatch):
        """Create a BulkDynamoDbRunner with mocked clients."""
        env_configs = MagicMock()
        env_configs.aws_region = 'us-east-1'
        env_configs.aws_account_id = '123456789012'

        with patch('runner.Clients') as mock_clients:
            mock_clients.return_value = MagicMock()
            r = BulkDynamoDbRunner(env_configs)
        return r

    def test_run_aborts_when_estimated_cost_exceeds_max(self, runner, capsys):
        """When XMaxEstimatedCostAllowed=5.00 and estimated cost is $10,
        run() should NOT call _start_glue_job and should log a message
        explaining the cost exceeds the limit."""
        args = {
            'XVersion': '1.0.0',
            'XMaxEstimatedCostAllowed': 5.00,
        }
        script_args = ['--verb', 'find', '--table', 'my-table']

        # Mock the cost estimation to return $10
        with patch.object(runner, '_get_glue_job_arguments', return_value={'--XAction': 'find'}), \
             patch.object(runner, '_assert_expected_script_args'), \
             patch.object(runner, '_start_glue_job') as mock_start, \
             patch('runner.get_estimated_cost', return_value=10.00):

            runner.run(args, script_args)

        # The Glue job should NOT have been started
        mock_start.assert_not_called()

        # An informative message should indicate cost exceeded limit
        output = capsys.readouterr()
        combined = output.out + output.err
        assert 'cost' in combined.lower() or 'exceed' in combined.lower() or \
               'max' in combined.lower(), \
            f"Expected cost-exceeds-limit message, got: {combined}"

    def test_run_proceeds_when_estimated_cost_within_max(self, runner):
        """When XMaxEstimatedCostAllowed=20.00 and estimated cost is $5,
        run() should proceed normally and call _start_glue_job."""
        args = {
            'XVersion': '1.0.0',
            'XMaxEstimatedCostAllowed': 20.00,
        }
        script_args = ['--verb', 'find', '--table', 'my-table']

        with patch.object(runner, '_get_glue_job_arguments', return_value={'--XAction': 'find'}), \
             patch.object(runner, '_assert_expected_script_args'), \
             patch.object(runner, '_start_glue_job', return_value='jr_123') as mock_start, \
             patch.object(runner, '_watch_for_interrupt'), \
             patch.object(runner, '_get_job_run_state', return_value='SUCCEEDED'), \
             patch.object(runner, '_get_job_run_error_message', return_value=None), \
             patch.object(runner, '_get_job_run_dpu', return_value=0), \
             patch('runner.get_estimated_cost', return_value=5.00):

            runner.run(args, script_args)

        mock_start.assert_called_once()

    def test_run_has_cost_estimation_capability(self, runner):
        """The runner must have a cost estimation mechanism that can be
        invoked before starting a job (prerequisite for max cost enforcement)."""
        # The runner should have a method or imported function for estimating cost
        import runner as runner_module
        assert hasattr(runner_module, 'get_estimated_cost') or \
               hasattr(runner, '_estimate_job_cost') or \
               hasattr(runner, '_check_cost_limit'), \
            "Runner must have a cost estimation function " \
            "(get_estimated_cost, _estimate_job_cost, or _check_cost_limit) " \
            "to support XMaxEstimatedCostAllowed"
