import tempfile
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Generator

from fastapi.param_functions import Depends

from backend.meteofrance.data_gouv_service import get_bulk_file_content
from backend.meteofrance.meteo_france_api_service import (
    fetch_daily_data_computation_results,
    get_last_mfapi_data_date,
    get_mf_access_token,
    launch_daily_data_computation,
)


class DataFileRepository:
    mf_api_token: set

    def __init__(self, mf_api_token=Depends(get_mf_access_token)):
        self.mf_api_token = mf_api_token

    def get_last_data_date(self) -> date:
        """
        Get latest data date available according to current time.

        Args:
        - None
        Returns:
        - date: last data date available on DataFile backend
        """
        return get_last_mfapi_data_date()

    @contextmanager
    def get_daily_file_path(self, begin_date: date) -> Generator[Path, None, None]:
        id_command = launch_daily_data_computation(
            begin_date=begin_date, token=self.mf_api_token
        )
        results = fetch_daily_data_computation_results(
            id_command=id_command, token=self.mf_api_token
        )
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            daily_file_name = "daily_file.csv"
            daily_file_path = Path(tmp_dir_name, daily_file_name)
            with open(daily_file_path, "w") as daily_file:
                daily_file.write(results)
            yield daily_file_path

    @contextmanager
    def get_bulk_file_path(self) -> Generator[Path, None, None]:
        content = get_bulk_file_content()
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            bulk_file_name = "bulk_file.csv.gz"
            bulk_file_path = Path(tmp_dir_name, bulk_file_name)
            with open(bulk_file_path, "wb") as bulk_file:
                bulk_file.write(content)
            yield bulk_file_path
