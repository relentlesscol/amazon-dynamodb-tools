"""Andon cord: abort signal for non-recoverable systemic errors.

When a worker hits a fatal error that cannot be resolved by retries (e.g.,
AccessDenied, ResourceNotFound, ValidationException), it "pulls the cord"
by writing a signal file to S3. Other workers check this signal at pagination
boundaries and abort gracefully, avoiding wasted work on a doomed job.

The name comes from Toyota's manufacturing andon cord — any worker on the
line can pull it to stop the entire line when they spot a defect.
"""
import json
import time

from botocore.exceptions import ClientError


FATAL_ERROR_CODES = frozenset([
    "AccessDeniedException",
    "ResourceNotFoundException",
    "ValidationException",
])

SIGNAL_FILENAME = "andon-signal.json"


def is_fatal_error(error_code: str) -> bool:
    return error_code in FATAL_ERROR_CODES


class NullAndonCord:
    """No-op cord used when andon_cord_config is not provided."""
    def pull(self, worker_id: str, reason: str):
        pass

    def is_pulled(self) -> bool:
        return False

    def cleanup(self):
        pass


class AndonCord:
    def __init__(self, s3_client, bucket: str, prefix: str):
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix if prefix.endswith("/") else prefix + "/"
        self._signal_key = f"{self._prefix}{SIGNAL_FILENAME}"
        self._pulled = False

    def pull(self, worker_id: str, reason: str):
        body = json.dumps({
            "worker_id": worker_id,
            "reason": reason,
            "timestamp": time.time(),
        })
        self._s3.put_object(Bucket=self._bucket, Key=self._signal_key, Body=body)
        self._pulled = True

    def is_pulled(self) -> bool:
        if self._pulled:
            return True

        try:
            self._s3.head_object(Bucket=self._bucket, Key=self._signal_key)
            self._pulled = True
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def cleanup(self):
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=self._signal_key)
        except Exception:
            pass
