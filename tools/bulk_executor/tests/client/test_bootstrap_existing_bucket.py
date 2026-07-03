"""Failing tests for issue #130: Allow bootstrap to specify an existing S3 bucket.

The user should be able to pass --XS3Bucket <bucket-name> to bootstrap to use
an existing S3 bucket instead of creating a new one. When this parameter is
provided, the bootstrap should:
1. NOT create a new bucket
2. Use the specified bucket name everywhere it needs a bucket
3. Still apply the secure transport policy to it
"""

import json
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def bootstrap():
    """Construct a BootstrapInfrastructure with all AWS clients mocked."""
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


class TestBootstrapExistingBucket:
    """When --XS3Bucket is provided, bootstrap should use it instead of creating one."""

    def test_specified_bucket_used_in_glue_job_default_arguments(self, bootstrap):
        """When XS3Bucket is provided, the Glue job's --s3-bucket-name default
        argument should be the user-specified bucket name."""
        with patch('infrastructure.bootstrap.is_existing_glue_job', return_value=True):
            bootstrap._create_or_update_glue_job({'XS3Bucket': 'my-existing-bucket'})

        update_kwargs = bootstrap.glue_client.update_job.call_args.kwargs['JobUpdate']
        assert update_kwargs['DefaultArguments']['--s3-bucket-name'] == 'my-existing-bucket'

    def test_specified_bucket_skips_bucket_creation(self, bootstrap):
        """When XS3Bucket is provided, _upload_job_root_to_s3 should NOT call
        create_bucket — the bucket already exists."""
        bootstrap._get_glue_job_details = MagicMock(return_value=None)
        bootstrap._bucket_exists = MagicMock(return_value=True)

        # Simulate that XS3Bucket was set — _get_glue_job_bucket_name should
        # return the user-provided bucket name
        bootstrap._user_specified_bucket = 'my-existing-bucket'
        bootstrap._get_glue_job_bucket_name = MagicMock(return_value='my-existing-bucket')

        bootstrap._upload_job_root_to_s3()

        # Should NOT try to create a bucket
        bootstrap.s3_client.create_bucket.assert_not_called()

    def test_get_glue_job_bucket_name_returns_user_bucket_when_specified(self, bootstrap):
        """_get_glue_job_bucket_name should return XS3Bucket value if one was
        stored during bootstrap initialization."""
        # This tests the NEW behavior: if the user specified a bucket,
        # use it without calling get_job to look up a persisted name
        # and without generating a random suffix.
        #
        # The current code has no way to accept a user-provided bucket name;
        # it always either reads from existing job or generates a random name.
        # This test will FAIL because _get_glue_job_bucket_name does not
        # check for a user-specified bucket.
        bootstrap._get_glue_job_details = MagicMock(return_value=None)

        # Set what would be stored from args during bootstrap()
        bootstrap._user_specified_bucket = 'my-existing-bucket'

        # Remove the auto-stub from fixture so we test real method
        if hasattr(bootstrap._get_glue_job_bucket_name, '_mock_name'):
            # Need to get the real method back
            with patch('infrastructure.bootstrap.Clients') as MockClients:
                MockClients.return_value = MagicMock()
                from infrastructure.bootstrap import BootstrapInfrastructure
                real_method = BootstrapInfrastructure._get_glue_job_bucket_name

            bootstrap._get_glue_job_bucket_name = lambda: real_method(bootstrap)

        result = bootstrap._get_glue_job_bucket_name()
        assert result == 'my-existing-bucket'
