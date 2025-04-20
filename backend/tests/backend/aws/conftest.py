import pytest
from moto import mock_aws

from backend.aws.dynamodb_service import get_aws_client, get_aws_resource


@pytest.fixture
def ddb_client():
    with mock_aws():
        yield get_aws_client()


@pytest.fixture
def ddb_resource():
    with mock_aws():
        yield get_aws_resource()
