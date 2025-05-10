from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Generator, Protocol

from core.entities import RainStore, TimespanId


class KeyValueDbProtocol(Protocol):
    async def get(self, keys: list[TimespanId]) -> dict[TimespanId, float]:
        """
        Get values corresponding to keys of KeyValueDb.

        We assume values exist in Db.

        Args:
        - keys, list[TimespanId]: keys to get values
        Returns:
        - dict[TimespanId, float]: dict with input keys & corresponding values
        """
        ...

    async def has(self, key: TimespanId) -> bool:
        """
        Checks if KeyValueDb has corresponding key.

        Args:
        - key, TimespanId: key to check existence in backend
        Returns:
        - bool: is the key in Db
        """
        ...

    async def post(self, rains: list[RainStore]) -> None:
        """
        Post new rain values to backend key value db.

        Args:
        - rains, list[RainStore]: rain values to store in backend
        Returns:
        - None
        """
        ...


class DataFileProtocol(Protocol):
    async def get_last_data_date(self) -> date: ...

    @contextmanager
    async def get_daily_file_path(
        self, begin_date: date
    ) -> Generator[Path, None, None]: ...

    @contextmanager
    async def get_bulk_file_path(self) -> Generator[Path, None, None]: ...
