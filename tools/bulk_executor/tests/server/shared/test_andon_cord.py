"""Tests for the andon cord (systemic error abort signal).

The andon cord allows any worker that hits a fatal, non-recoverable error
to signal all other workers to abort gracefully. Uses S3 as the coordination
plane (same pattern as the rate limiter).
"""
import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

import sys
import pathlib

_server_src = pathlib.Path(__file__).resolve().parents[3] / "server" / "src"
sys.path.insert(0, str(_server_src))

from python_modules.shared.andon_cord import AndonCord


class TestAndonCordPull:
    """When a worker pulls the andon cord, the S3 signal file is written."""

    def test_pull_writes_signal_to_s3(self):
        mock_s3 = MagicMock()
        cord = AndonCord(s3_client=mock_s3, bucket="test-bucket", prefix="job-123/")

        cord.pull(worker_id="worker-42", reason="AccessDeniedException on target table")

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"] == "job-123/andon-signal.json"

        body = json.loads(call_kwargs["Body"])
        assert body["worker_id"] == "worker-42"
        assert body["reason"] == "AccessDeniedException on target table"
        assert "timestamp" in body

    def test_pull_sets_local_flag_immediately(self):
        from botocore.exceptions import ClientError
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        cord = AndonCord(s3_client=mock_s3, bucket="test-bucket", prefix="job-123/")

        assert not cord.is_pulled()
        cord.pull(worker_id="worker-1", reason="fatal")
        assert cord.is_pulled()


class TestAndonCordCheck:
    """Workers check the cord periodically; once pulled, they see it."""

    def test_is_pulled_returns_false_when_no_signal(self):
        mock_s3 = MagicMock()
        mock_s3.head_object.side_effect = mock_s3.exceptions.NoSuchKey({}, "HeadObject")
        # Simulate ClientError for NoSuchKey
        from botocore.exceptions import ClientError
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        cord = AndonCord(s3_client=mock_s3, bucket="test-bucket", prefix="job-123/")

        assert not cord.is_pulled()

    def test_is_pulled_returns_true_when_signal_exists(self):
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {}  # Signal file exists
        cord = AndonCord(s3_client=mock_s3, bucket="test-bucket", prefix="job-123/")

        assert cord.is_pulled()

    def test_is_pulled_caches_after_first_true(self):
        """Once we know the cord is pulled, stop hitting S3."""
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {}
        cord = AndonCord(s3_client=mock_s3, bucket="test-bucket", prefix="job-123/")

        cord.is_pulled()
        cord.is_pulled()
        cord.is_pulled()

        # Only one S3 call needed — after that, local cache kicks in
        assert mock_s3.head_object.call_count == 1


class TestAndonCordCleanup:
    """The aggregator/driver cleans up the signal file after the job."""

    def test_cleanup_deletes_signal_file(self):
        mock_s3 = MagicMock()
        cord = AndonCord(s3_client=mock_s3, bucket="test-bucket", prefix="job-123/")

        cord.cleanup()

        mock_s3.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="job-123/andon-signal.json"
        )


class TestAndonCordIntegration:
    """End-to-end: one worker pulls, another worker sees it."""

    def test_cross_worker_signal_propagation(self):
        """Simulates two workers sharing the same S3 mock."""
        shared_store = {}

        def mock_put_object(**kwargs):
            shared_store[kwargs["Key"]] = kwargs["Body"]

        def mock_head_object(**kwargs):
            from botocore.exceptions import ClientError
            if kwargs["Key"] in shared_store:
                return {}
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )

        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = mock_put_object
        mock_s3.head_object.side_effect = mock_head_object

        cord_worker_a = AndonCord(s3_client=mock_s3, bucket="b", prefix="j/")
        cord_worker_b = AndonCord(s3_client=mock_s3, bucket="b", prefix="j/")

        assert not cord_worker_b.is_pulled()

        cord_worker_a.pull(worker_id="A", reason="ResourceNotFoundException")

        assert cord_worker_b.is_pulled()


class TestFatalErrorClassification:
    """Verify which errors are considered fatal/non-recoverable."""

    def test_known_fatal_errors(self):
        from python_modules.shared.andon_cord import is_fatal_error

        fatal_codes = [
            "AccessDeniedException",
            "ResourceNotFoundException",
            "ValidationException",
        ]
        for code in fatal_codes:
            assert is_fatal_error(code), f"{code} should be fatal"

    def test_recoverable_errors_are_not_fatal(self):
        from python_modules.shared.andon_cord import is_fatal_error

        recoverable_codes = [
            "ProvisionedThroughputExceededException",
            "InternalServerError",
            "ServiceUnavailable",
        ]
        for code in recoverable_codes:
            assert not is_fatal_error(code), f"{code} should NOT be fatal"
