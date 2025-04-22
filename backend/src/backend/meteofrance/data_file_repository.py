import tempfile
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from typing import AsyncGenerator

from aiohttp import ClientSession
from fastapi.param_functions import Depends

from backend.meteofrance.data_gouv_service import get_bulk_file_content
from backend.meteofrance.meteo_france_api_service import (
    fetch_daily_data_computation_results,
    get_last_mfapi_data_date,
    get_mf_access_token,
    launch_daily_data_computation,
)


class DataFileRepository:
    mf_api_token: str
    session: ClientSession

    def __init__(
        self, mf_api_token=Depends(get_mf_access_token), session=Depends(ClientSession)
    ):
        self.mf_api_token = mf_api_token
        self.session = session

    async def get_last_data_date(self) -> date:
        """
        Get latest data date available according to current time.

        Args:
        - None
        Returns:
        - date: last data date available on DataFile backend
        """
        return await get_last_mfapi_data_date()

    @asynccontextmanager
    async def get_daily_file_path(self, begin_date: date) -> AsyncGenerator[Path]:
        """
        Get daily data file and yield its path.

        Args:
        - begin_date, date : date to begin daily data fetch
        Yields:
        - Path: temporary path of daily data fetched file
        """
        id_command = await launch_daily_data_computation(
            session=self.session, begin_date=begin_date, token=self.mf_api_token
        )
        results = await fetch_daily_data_computation_results(
            session=self.session, id_command=id_command, token=self.mf_api_token
        )
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            daily_file_name = "daily_file.csv"
            daily_file_path = Path(tmp_dir_name, daily_file_name)
            with open(daily_file_path, "w") as daily_file:
                daily_file.write(results)
            yield daily_file_path

    @asynccontextmanager
    async def get_bulk_file_path(self) -> AsyncGenerator[Path]:
        """
        Get bulk data file and yield its path.

        Args:
        - None
        Yields:
        - Path: temporary path of bulk data fetched file
        """
        content = get_bulk_file_content(session=self.session)
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            bulk_file_name = "bulk_file.csv.gz"
            bulk_file_path = Path(tmp_dir_name, bulk_file_name)
            with open(bulk_file_path, "wb") as bulk_file:
                bulk_file.write(content)
            yield bulk_file_path
