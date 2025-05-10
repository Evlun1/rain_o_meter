import datetime as dt
import json

import pytest
from httpx import ASGITransport, AsyncClient

from api import app, get_last_data_day
from core.entities import RainCompleteInfo
from core.exceptions import AlreadyAddedData, AlreadyInitialized


@pytest.fixture
async def async_client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest.mark.anyio
async def test_get_last_data_day(mocker):
    expected_date = dt.date(2025, 4, 1)
    service_mock = mocker.patch(
        "api.core_service.get_last_data_date", return_value=expected_date
    )
    result = await get_last_data_day()
    assert result == expected_date
    service_mock.assert_called_once_with(mocker.ANY)


@pytest.mark.anyio
async def test_get(mocker, async_client):
    expected_date = dt.date(2025, 4, 1)
    mocker.patch("api.core_service.get_last_data_date", return_value=expected_date)
    expected_data = RainCompleteInfo(
        last_day=dt.date(2025, 4, 1),
        last_day_rain_mm=0,
        month_beg=dt.date(2025, 4, 1),
        since_month_beg_mm=0,
        mean_month_beg_mm=2,
        prev_30_days=dt.date(2025, 3, 2),
        last_31_days_mm=56.5,
        mean_31_days_mm=72.3,
    )
    service_mock = mocker.patch("api.core_service.get_data", return_value=expected_data)
    response = await async_client.get("/")
    assert response.status_code == 200
    assert response.json() == json.loads(expected_data.model_dump_json())
    service_mock.assert_called_once_with(mocker.ANY, expected_date)


@pytest.mark.anyio
class TestAdd:
    async def test_add_normal_case(self, mocker, async_client):
        expected_date = dt.date(2025, 4, 1)
        mocker.patch("api.core_service.get_last_data_date", return_value=expected_date)
        service_mock = mocker.patch("api.core_service.fetch_daily_data_if_not_in_cache")
        response = await async_client.get("/add")
        assert response.status_code == 201
        service_mock.assert_called_once_with(mocker.ANY, mocker.ANY, expected_date)

    async def test_add_already_added_data_case(self, mocker, async_client):
        expected_date = dt.date(2025, 4, 1)
        mocker.patch("api.core_service.get_last_data_date", return_value=expected_date)
        service_mock = mocker.patch(
            "api.core_service.fetch_daily_data_if_not_in_cache",
            side_effect=AlreadyAddedData,
        )
        response = await async_client.get("/add")
        assert response.status_code == 409
        assert response.json() == {"detail": "Data is already in backend."}
        service_mock.assert_called_once_with(mocker.ANY, mocker.ANY, expected_date)


@pytest.mark.anyio
class TestInitialize:
    async def test_initialize_normal_case(self, mocker, async_client, settings):
        mocker.patch("api.settings", settings)
        service_mock = mocker.patch("api.core_service.initialize_mean_data")
        response = await async_client.get("/initialize")
        assert response.status_code == 201
        service_mock.assert_called_once_with(mocker.ANY, mocker.ANY, 2020, 2021)

    async def test_add_already_initialized_data_case(
        self, mocker, async_client, settings
    ):
        mocker.patch("api.settings", settings)
        service_mock = mocker.patch(
            "api.core_service.initialize_mean_data",
            side_effect=AlreadyInitialized,
        )
        response = await async_client.get("/initialize")
        assert response.status_code == 409
        assert response.json() == {"detail": "Key value DB is already initialized."}
        service_mock.assert_called_once_with(mocker.ANY, mocker.ANY, 2020, 2021)
