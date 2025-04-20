import datetime as dt
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from freezegun import freeze_time
from requests.models import Response

from backend.meteofrance.meteo_france_api_service import (
    fetch_daily_data_computation_results,
    get_last_mfapi_data_date,
    get_mf_access_token,
    launch_daily_data_computation,
)


@pytest.mark.parametrize(
    "test_date,expected",
    [
        ("2025-04-03 11:58:45", dt.date(2025, 4, 2)),
        ("2025-04-03 08:12:33", dt.date(2025, 4, 1)),
    ],
)
def test_get_last_mfapi_data_date(test_date, expected):
    with freeze_time(test_date):
        assert get_last_mfapi_data_date() == expected


def test_get_mf_access_token(mocker, settings):
    expected = "toktok"
    response = Mock(spec=Response)
    response.json.return_value = {"access_token": expected}
    post_patch = mocker.patch(
        "backend.meteofrance.meteo_france_api_service.rq.post",
        return_value=response,
    )
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    result = get_mf_access_token()
    post_patch.assert_called_once_with(
        "www.testtoken.com",
        data={"grant_type": "client_credentials"},
        allow_redirects=False,
        headers={"Authorization": "Basic 1234ab"},
    )
    assert result == expected


def test_launch_daily_data_computation(mocker, settings):
    input_begin_date = dt.date(2025, 3, 15)
    mocked_end_date = dt.date(2025, 4, 1)
    input_token = "1234ab"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    last_date_patch = mocker.patch(
        "backend.meteofrance.meteo_france_api_service.get_last_mfapi_data_date",
        return_value=mocked_end_date,
    )
    expected_id = "id5678"
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {
        "elaboreProduitAvecDemandeResponse": {"return": expected_id}
    }
    get_patch = mocker.patch(
        "backend.meteofrance.meteo_france_api_service.rq.get",
        return_value=response,
    )

    result = launch_daily_data_computation(
        begin_date=input_begin_date, token=input_token
    )

    last_date_patch.assert_called_once()
    get_patch.assert_called_once_with(
        url="www.mfapp.com/commande-station/quotidienne",
        params={
            "id-station": "75114001",
            "date-deb-periode": "2025-03-15T00:00:00Z",
            "date-fin-periode": "2025-04-01T00:00:00Z",
        },
        headers={"Authorization": "Bearer 1234ab"},
    )
    assert result == expected_id


def test_launch_daily_data_computation_raise_if_error(mocker, settings):
    input_begin_date = dt.date(2025, 3, 15)
    mocked_end_date = dt.date(2025, 4, 1)
    input_token = "1234ab"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    mocker.patch(
        "backend.meteofrance.meteo_france_api_service.get_last_mfapi_data_date",
        return_value=mocked_end_date,
    )
    response = Mock(spec=Response)
    response.status_code = 500
    response.text = "Internal server error"
    mocker.patch(
        "backend.meteofrance.meteo_france_api_service.rq.get",
        return_value=response,
    )

    with pytest.raises(HTTPException):
        launch_daily_data_computation(begin_date=input_begin_date, token=input_token)


def test_fetch_daily_data_computation_results(mocker, settings):
    input_id_command = "id5678"
    input_token = "1234ab"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    expected_content = "RR\n55"
    response = Mock(spec=Response)
    response.status_code = 200
    response.text = expected_content
    get_patch = mocker.patch(
        "backend.meteofrance.meteo_france_api_service.rq.get",
        return_value=response,
    )

    result = fetch_daily_data_computation_results(
        id_command=input_id_command, token=input_token
    )

    get_patch.assert_called_once_with(
        url="www.mfapp.com/commande/fichier",
        params={"id-cmde": "id5678"},
        headers={"Authorization": "Bearer 1234ab"},
    )
    assert result == expected_content


def test_fetch_daily_data_computation_results_raise_if_error(mocker, settings):
    input_id_command = "id5678"
    input_token = "1234ab"
    mocker.patch("backend.meteofrance.meteo_france_api_service.settings", settings)
    response = Mock(spec=Response)
    response.status_code = 500
    response.text = "Internal server error"
    mocker.patch(
        "backend.meteofrance.meteo_france_api_service.rq.get",
        return_value=response,
    )

    with pytest.raises(HTTPException):
        fetch_daily_data_computation_results(
            id_command=input_id_command, token=input_token
        )
