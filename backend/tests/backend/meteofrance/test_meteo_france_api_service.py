import datetime as dt

import pytest
from fastapi import HTTPException
from freezegun import freeze_time

from backend.meteofrance.meteo_france_api_service import (
    fetch_daily_data_computation_results,
    get_last_mfapi_data_date,
    get_mf_access_token,
    launch_daily_data_computation,
)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "test_date,expected",
    [
        ("2025-04-03 11:58:45", dt.date(2025, 4, 2)),
        ("2025-04-03 08:12:33", dt.date(2025, 4, 1)),
    ],
)
async def test_get_last_mfapi_data_date(test_date, expected):
    with freeze_time(test_date):
        assert await get_last_mfapi_data_date() == expected


@pytest.mark.anyio
async def test_get_mf_access_token(mocker, settings, aiohttp_session, mock_responses):
    expected = "toktok"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    mock_responses.post(
        "www.testtoken.com", status=200, payload={"access_token": expected}
    )
    result = await get_mf_access_token(session=aiohttp_session)
    assert result == expected


@pytest.mark.anyio
async def test_launch_daily_data_computation(
    mocker, settings, aiohttp_session, mock_responses
):
    input_begin_date = dt.date(2025, 3, 15)
    mocked_end_date = dt.date(2025, 4, 1)
    input_token = "1234ab"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    last_date_patch = mocker.patch(
        "backend.meteofrance.meteo_france_api_service.get_last_mfapi_data_date",
        return_value=mocked_end_date,
    )
    expected_id = "id5678"
    mock_responses.get(
        "www.mfapp.com/commande-station/quotidienne?date-deb-periode=2025-03-15T00:00:00Z&date-fin-periode=2025-04-01T00:00:00Z&id-station=75114001",
        status=200,
        payload={"elaboreProduitAvecDemandeResponse": {"return": expected_id}},
    )

    result = await launch_daily_data_computation(
        session=aiohttp_session, begin_date=input_begin_date, token=input_token
    )

    last_date_patch.assert_called_once()
    assert result == expected_id


@pytest.mark.anyio
async def test_launch_daily_data_computation_raise_if_error(
    mocker, settings, aiohttp_session, mock_responses
):
    input_begin_date = dt.date(2025, 3, 15)
    mocked_end_date = dt.date(2025, 4, 1)
    input_token = "1234ab"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    mocker.patch(
        "backend.meteofrance.meteo_france_api_service.get_last_mfapi_data_date",
        return_value=mocked_end_date,
    )
    mock_responses.get(
        "www.mfapp.com/commande-station/quotidienne?date-deb-periode=2025-03-15T00:00:00Z&date-fin-periode=2025-04-01T00:00:00Z&id-station=75114001",
        status=500,
        body="Internal server error",
    )

    with pytest.raises(HTTPException):
        await launch_daily_data_computation(
            session=aiohttp_session, begin_date=input_begin_date, token=input_token
        )


@pytest.mark.anyio
async def test_fetch_daily_data_computation_results(
    mocker, settings, aiohttp_session, mock_responses
):
    input_id_command = "id5678"
    input_token = "1234ab"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    expected_content = "RR\n55"
    mock_responses.get(
        url=f"www.mfapp.com/commande/fichier?id-cmde={input_id_command}",
        status=200,
        body=expected_content,
    )

    result = await fetch_daily_data_computation_results(
        session=aiohttp_session, id_command=input_id_command, token=input_token
    )

    assert result == expected_content


@pytest.mark.anyio
async def test_fetch_daily_data_computation_results_raise_if_error(
    mocker, settings, aiohttp_session, mock_responses
):
    input_id_command = "id5678"
    input_token = "1234ab"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    mock_responses.get(
        url=f"www.mfapp.com/commande/fichier?id-cmde={input_id_command}",
        status=500,
        body="Internal server error",
    )

    with pytest.raises(HTTPException):
        await fetch_daily_data_computation_results(
            session=aiohttp_session, id_command=input_id_command, token=input_token
        )
