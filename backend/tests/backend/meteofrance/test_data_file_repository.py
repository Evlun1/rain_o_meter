import datetime as dt

import pytest

from backend.meteofrance.data_file_repository import DataFileRepository


@pytest.fixture
def data_file_repository() -> DataFileRepository:
    return DataFileRepository(mf_api_token="id1234")


def test_get_last_data_date(mocker, data_file_repository):
    expected_date = dt.date(2025, 4, 1)
    mock = mocker.patch(
        "backend.meteofrance.data_file_repository.get_last_mfapi_data_date",
        return_value=expected_date,
    )
    result = data_file_repository.get_last_data_date()
    assert result == expected_date
    mock.assert_called_once()


def test_get_daily_file_path(mocker, data_file_repository):
    input_begin_date = dt.date(2025, 4, 1)
    expected_results = "RR\n55"
    launch_mock = mocker.patch(
        "backend.meteofrance.data_file_repository.launch_daily_data_computation",
        return_value="id9",
    )
    results_mock = mocker.patch(
        "backend.meteofrance.data_file_repository.fetch_daily_data_computation_results",
        return_value=expected_results,
    )

    with data_file_repository.get_daily_file_path(begin_date=input_begin_date) as dfp:
        with open(dfp, "r") as test_file:
            assert test_file.read() == expected_results

    launch_mock.assert_called_once_with(begin_date=input_begin_date, token="id1234")
    results_mock.assert_called_once_with(id_command="id9", token="id1234")


def test_get_bulk_file_path(mocker, data_file_repository):
    expected_contents = bytes("RR\n55", "utf8")
    content_mock = mocker.patch(
        "backend.meteofrance.data_file_repository.get_bulk_file_content",
        return_value=expected_contents,
    )

    with data_file_repository.get_bulk_file_path() as bfp:
        with open(bfp, "rb") as test_file:
            assert test_file.read() == expected_contents

    content_mock.assert_called_once()
