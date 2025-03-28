import pytest


@pytest.fixture()
def mock_module(mocker):
    return lambda name, spec: mocker.Mock(name=name, spec=spec)
