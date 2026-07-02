"""Test for issue #130: Allow bootstrap to specify an existing S3 bucket.

Some customers can't give bootstrap enough permissions to create an S3
bucket. They should be able to pass --XBucket <name> to use an existing
bucket instead of creating a new one.
"""

from unittest.mock import MagicMock, patch, call

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def bootstrap():
    """Construct a BootstrapInfrastructure with mocked AWS clients."""
    with patch('infrastructure.bootstrap.Clients') as MockClients:
        clients = MagicMock()
        clients.iam_client = MagicMock()
        clients.s3_client = MagicMock()
        clients.glue_client = MagicMock()
        clients.logs_client = MagicMock()
        MockClients.return_value = clients

        from infrastructure.bootstrap import BootstrapInfrastructure
        env = MagicMock(aws_region='us-east-1', aws_account_id='123456789012')
        instance = BootstrapInfrastructure(env)

    return instance


class TestExistingBucketSupport:
    """Bootstrap should accept an existing S3 bucket via --XBucket."""

    def test_uses_provided_bucket_name_instead_of_creating(self, bootstrap):
        """When XBucket is provided, bootstrap should NOT create a new bucket."""
        args = {'XBucket': 'my-existing-bucket'}

        bucket_name = bootstrap._resolve_s3_bucket(args)

        assert bucket_name == 'my-existing-bucket'
        # Should NOT call create_bucket
        bootstrap.s3_client.create_bucket.assert_not_called()

    def test_creates_bucket_when_xbucket_not_specified(self, bootstrap):
        """Without XBucket, the existing create-bucket behavior is preserved."""
        args = {}  # No XBucket

        # Should call some creation logic (existing behavior)
        # This test just verifies the function exists and handles the no-arg case
        bootstrap._resolve_s3_bucket(args)
        # The create path should be invoked (existing behavior)
        assert bootstrap.s3_client.create_bucket.called or bootstrap.s3_client.head_bucket.called

    def test_validates_existing_bucket_is_accessible(self, bootstrap):
        """When XBucket is provided, bootstrap should verify it's accessible."""
        bootstrap.s3_client.head_bucket = MagicMock()  # bucket exists
        args = {'XBucket': 'accessible-bucket'}

        bucket_name = bootstrap._resolve_s3_bucket(args)
        assert bucket_name == 'accessible-bucket'
        bootstrap.s3_client.head_bucket.assert_called_with(Bucket='accessible-bucket')

    def test_raises_when_existing_bucket_not_accessible(self, bootstrap):
        """If XBucket refers to a non-existent/inaccessible bucket, raise."""
        error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
        bootstrap.s3_client.head_bucket.side_effect = ClientError(
            error_response, 'HeadBucket'
        )
        args = {'XBucket': 'nonexistent-bucket'}

        with pytest.raises((ClientError, ValueError, SystemExit)):
            bootstrap._resolve_s3_bucket(args)
