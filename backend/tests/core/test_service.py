from contextlib import contextmanager
from unittest.mock import call
from core.service import (
    get_data,
    _compute_daily_data,
    fetch_daily_data_if_not_in_cache,
    _preprocess_bulk_data,
    _get_mean_data_between_two_mon_day_dates,
    _compute_history_means,
    initialize_mean_data,
    get_last_data_date,
)
from core.entities import RainCompleteInfo, RainStore
from core.protocol import KeyValueDbProtocol, DataFileProtocol
from pathlib import Path
from pandera.errors import SchemaError
from core.exceptions import AlreadyAddedData, AlreadyInitialized


import datetime as dt
import pytest
import polars as pl
from polars.testing import assert_frame_equal


@pytest.fixture()
def data_file_repo(mock_module):
    return mock_module("core.protocol", DataFileProtocol)


@pytest.fixture()
def key_value_db_repo(mock_module):
    return mock_module("core.protocol", KeyValueDbProtocol)


class TestGetData:
    def test_get_data(self, key_value_db_repo):
        input_last_data_date = dt.date(2025, 4, 15)
        key_value_db_repo.get.return_value = {
            "20250415-20250415": 0,
            "20250401-20250415": 10,
            "20250316-20250415": 20,
            "M0401-M0415": 40,
            "M0316-M0415": 50,
        }

        result = get_data(
            key_value_db_repo=key_value_db_repo, last_data_day=input_last_data_date
        )

        expected = RainCompleteInfo(
            last_day=input_last_data_date,
            last_day_rain_mm=0,
            month_beg=dt.date(2025, 4, 1),
            since_month_beg_mm=10,
            mean_month_beg_mm=40,
            prev_30_days=dt.date(2025, 3, 16),
            last_31_days_mm=20,
            mean_31_days_mm=50,
        )
        key_value_db_repo.get.assert_called_once_with(
            keys=[
                "20250415-20250415",
                "20250401-20250415",
                "20250316-20250415",
                "M0401-M0415",
                "M0316-M0415",
            ]
        )
        assert result == expected


class TestComputeDailyData:
    def test_compute_daily_data(self):
        input_file_path = Path(__file__).parent.joinpath(
            "resources", "input_compute_daily_data.csv"
        )
        input_this_day = dt.date(2025, 4, 2)
        expected_this_day = 5
        expected_this_month = 10.5
        expected_last_31_days = 14.5
        result_this_day, result_this_month, result_last_31_days = _compute_daily_data(
            input_file_path, input_this_day
        )
        assert result_this_day == expected_this_day
        assert expected_this_month == result_this_month
        assert expected_last_31_days == result_last_31_days

    def test_compute_daily_data_should_raise_validation_error(self):
        input_file_path = Path(__file__).parent.joinpath(
            "resources", "input_compute_daily_data_wrong_format.csv"
        )
        input_this_day = dt.date(2025, 4, 2)
        with pytest.raises(SchemaError):
            _compute_daily_data(input_file_path, input_this_day)


class TestFetchDailyDataIfNotInCache:
    def test_fetch_daily_data_not_in_cache(self, data_file_repo, key_value_db_repo):
        input_last_data_day = dt.date(2025, 4, 2)
        key_value_db_repo.has.return_value = False

        @contextmanager
        def mock_daily_file_path(begin_date):
            yield Path(__file__).parent.joinpath(
                "resources", "input_compute_daily_data.csv"
            )

        data_file_repo.get_daily_file_path = mock_daily_file_path
        fetch_daily_data_if_not_in_cache(
            key_value_db_repo, data_file_repo, input_last_data_day
        )
        key_value_db_repo.post.assert_called_once_with(
            [
                RainStore(timespan_id="20250402-20250402", rain_mm=5),
                RainStore(timespan_id="20250401-20250402", rain_mm=10.5),
                RainStore(timespan_id="20250303-20250402", rain_mm=14.5),
            ]
        )

    def test_fetch_daily_data_already_in_cache(self, data_file_repo, key_value_db_repo):
        input_last_data_day = dt.date(2025, 4, 2)
        key_value_db_repo.has.return_value = True
        with pytest.raises(AlreadyAddedData):
            fetch_daily_data_if_not_in_cache(
                key_value_db_repo, data_file_repo, input_last_data_day
            )


class TestPreprocessBulkData:
    def test_preprocess_bulk_data(self, mocker):
        input_df = pl.DataFrame(
            data={
                "station_id": [1, 1, 1, 2],
                "date": [20250410, 20250411, 20250412, 20250413],
                "rainfall_mm": [0, 1.5, 2, 3],
            },
            schema={"station_id": int, "date": int, "rainfall_mm": float},
        )
        input_begin_date = dt.date(2025, 4, 11)
        input_end_date = dt.date(2025, 4, 15)
        expected_df = pl.DataFrame(
            data={
                "date": [dt.date(2025, 4, 11), dt.date(2025, 4, 12)],
                "rainfall_mm": [1.5, 2],
                "month": [4, 4],
                "day": [11, 12],
            },
            schema={
                "date": dt.date,
                "rainfall_mm": float,
                "month": pl.Int8,
                "day": pl.Int8,
            },
        )
        mocker.patch("core.service.STATION_ID", 1)
        result = _preprocess_bulk_data(input_df, input_begin_date, input_end_date)
        assert_frame_equal(result, expected_df)


class TestGetMeanDataBetweenTwoMonDayDates:
    def test_get_mean_data_between_two_mon_day_dates_normal_case(self):
        input_beg_mon = 3
        input_beg_day = 30
        input_end_mon = 4
        input_end_day = 2
        input_number_of_years = 2
        input_df = pl.DataFrame(
            data={
                "date": [
                    dt.date(2024, 3, 29),
                    dt.date(2024, 3, 30),
                    dt.date(2024, 4, 1),
                    dt.date(2024, 4, 2),
                    dt.date(2025, 3, 30),
                    dt.date(2025, 4, 1),
                    dt.date(2025, 4, 2),
                    dt.date(2025, 4, 3),
                ],
                "rainfall_mm": [0.5, 1, 2, 3, 4, 5, 6, 7],
                "month": [3, 3, 4, 4, 3, 4, 4, 4],
                "day": [29, 30, 1, 2, 30, 1, 2, 3],
            }
        )
        expected_mean = 10.5
        result = _get_mean_data_between_two_mon_day_dates(
            input_df,
            input_beg_mon,
            input_beg_day,
            input_end_mon,
            input_end_day,
            input_number_of_years,
        )
        assert result == expected_mean

    def test_get_mean_data_between_two_mon_day_dates_between_years(self):
        input_beg_mon = 12
        input_beg_day = 31
        input_end_mon = 1
        input_end_day = 2
        input_number_of_years = 2
        input_df = pl.DataFrame(
            data={
                "date": [
                    dt.date(2023, 12, 30),
                    dt.date(2023, 12, 31),
                    dt.date(2024, 1, 1),
                    dt.date(2024, 1, 2),
                    dt.date(2024, 12, 31),
                    dt.date(2025, 1, 1),
                    dt.date(2025, 1, 2),
                    dt.date(2025, 1, 3),
                ],
                "rainfall_mm": [0.5, 1, 2, 3, 4, 5, 6, 7],
                "month": [12, 12, 1, 1, 12, 1, 1, 1],
                "day": [30, 31, 1, 2, 31, 1, 2, 3],
            }
        )
        expected_mean = 10.5
        result = _get_mean_data_between_two_mon_day_dates(
            input_df,
            input_beg_mon,
            input_beg_day,
            input_end_mon,
            input_end_day,
            input_number_of_years,
        )
        assert result == expected_mean


class TestComputeHistoryMeans:
    def test_compute_history_means(self):
        input_df = pl.DataFrame(
            data={
                "date": [
                    dt.date(2024, 3, 29),
                    dt.date(2024, 3, 30),
                    dt.date(2024, 4, 1),
                    dt.date(2024, 4, 2),
                    dt.date(2025, 3, 30),
                    dt.date(2025, 4, 1),
                    dt.date(2025, 4, 2),
                    dt.date(2025, 4, 3),
                ],
                "rainfall_mm": [0.5, 1, 2, 3, 4, 5, 6, 7],
                "month": [3, 3, 4, 4, 3, 4, 4, 4],
                "day": [29, 30, 1, 2, 30, 1, 2, 3],
            }
        )
        input_this_day = dt.date(2000, 4, 2)
        input_number_of_years = 2
        result_rain_1, result_rain_2 = _compute_history_means(
            input_df, input_this_day, input_number_of_years
        )
        expected_rain_1 = RainStore(timespan_id="M0401-M0402", rain_mm=8)
        expected_rain_2 = RainStore(timespan_id="M0303-M0402", rain_mm=10.8)
        assert result_rain_1 == expected_rain_1
        assert result_rain_2 == expected_rain_2


class TestInitializeMeanData:
    def test_initialize_mean_data(self, mocker, data_file_repo, key_value_db_repo):
        input_year_beg_incl = 2020
        input_year_end_incl = 2020
        key_value_db_repo.has.return_value = False

        @contextmanager
        def mock_get_bulk_file_path():
            yield Path(__file__).parent.joinpath(
                "resources", "input_init_mean_data.csv"
            )

        data_file_repo.get_bulk_file_path = mock_get_bulk_file_path
        mocker.patch("core.service.STATION_ID", 75000001)
        compute_history_patch = mocker.patch(
            "core.service._compute_history_means", return_value=(5, 5)
        )

        initialize_mean_data(
            key_value_db_repo, data_file_repo, input_year_beg_incl, input_year_end_incl
        )

        assert compute_history_patch.call_args_list[0] == call(
            mocker.ANY, mocker.ANY, dt.date(2000, 1, 1), 1
        )
        assert compute_history_patch.call_count == 366
        key_value_db_repo.post.assert_called_once_with(rains=[5] * 732)

    def test_initialize_mean_data_raise_if_already_init(
        self, data_file_repo, key_value_db_repo
    ):
        input_year_beg_incl = 2020
        input_year_end_incl = 2020
        key_value_db_repo.has.return_value = True

        with pytest.raises(AlreadyInitialized):
            initialize_mean_data(
                key_value_db_repo,
                data_file_repo,
                input_year_beg_incl,
                input_year_end_incl,
            )


class TestGetLastDataDate:
    def test_get_last_data_date(self, data_file_repo):
        data_file_repo.get_last_data_date.return_value = dt.date(2025, 4, 1)
        result = get_last_data_date(data_file_repo)
        data_file_repo.get_last_data_date.assert_called_once()
        assert result == dt.date(2025, 4, 1)
