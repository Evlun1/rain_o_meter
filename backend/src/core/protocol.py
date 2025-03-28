from typing import Generator, Protocol

from core.entities import RainStore, TimespanId
from datetime import date
from pathlib import Path

from contextlib import contextmanager


class KeyValueDbProtocol(Protocol):
    def get(self, keys: list[TimespanId]) -> dict[TimespanId, float]: ...

    def has(self, key: TimespanId) -> bool: ...

    def post(self, rains: list[RainStore]) -> None: ...


class DataFileProtocol(Protocol):
    def get_last_data_date(self) -> date: ...

    @contextmanager
    def get_daily_file_path(self, begin_date: date) -> Generator[Path, None, None]: ...

    @contextmanager
    def get_bulk_file_path(self) -> Generator[Path, None, None]: ...
