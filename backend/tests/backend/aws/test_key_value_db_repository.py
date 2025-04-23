import pytest
from aioboto3 import Session

from backend.aws.key_value_db_repository import KeyValueDbRepository
from core.entities import RainStore


@pytest.fixture(scope="module")
def key_value_db_repository(aws_session: Session) -> KeyValueDbRepository:
    return KeyValueDbRepository(session=aws_session)


@pytest.mark.anyio
async def test_get(mocker, key_value_db_repository):
    input_keys = ["20250401-20250410", "M0401-M0410"]
    mocked_values = [0, 10]
    get_mock = mocker.patch(
        "backend.aws.key_value_db_repository.get_items", return_value=mocked_values
    )
    result = await key_value_db_repository.get(input_keys)
    expected = {"20250401-20250410": 0, "M0401-M0410": 10}
    assert result == expected
    get_mock.assert_called_once_with(ddb_client=mocker.ANY, keys=input_keys)


@pytest.mark.anyio
async def test_has(mocker, key_value_db_repository):
    input_key = "20250401-20250410"
    has_mock = mocker.patch(
        "backend.aws.key_value_db_repository.has_item", return_value=True
    )
    assert await key_value_db_repository.has(key=input_key) is True
    has_mock.assert_called_once_with(ddb_client=mocker.ANY, key=input_key)


@pytest.mark.anyio
async def test_post(mocker, key_value_db_repository):
    input_rains = [
        RainStore(timespan_id="20250401-20250410", rain_mm=0),
        RainStore(timespan_id="M0401-M0410", rain_mm=10),
    ]
    write_mock = mocker.patch("backend.aws.key_value_db_repository.write_items")
    await key_value_db_repository.post(input_rains)
    write_mock.assert_called_once_with(
        ddb_resource=mocker.ANY, items={"20250401-20250410": 0, "M0401-M0410": 10}
    )
