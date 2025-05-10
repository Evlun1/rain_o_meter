from typing import Self

from aioboto3 import Session
from fastapi.param_functions import Depends

from backend.aws.dynamodb_service import (
    get_aws_session,
    get_items,
    has_item,
    write_items,
)
from core.entities import RainStore, TimespanId
from settings import get_api_settings

settings = get_api_settings()


class KeyValueDbRepository:
    session: Session

    def __init__(
        self,
        session: Session = Depends(get_aws_session),
    ) -> Self:
        self.session = session
        self.endpoint_url = (
            {"endpoint_url": settings.aws_endpoint} if settings.aws_endpoint else {}
        )

    async def get(self, keys: list[TimespanId]) -> dict[TimespanId, float]:
        """
        Get values corresponding to keys of KeyValueDb.

        We assume values exist in Db.

        Args:
        - keys, list[TimespanId]: keys to get values
        Returns:
        - dict[TimespanId, float]: dict with input keys & corresponding values
        """
        async with self.session.client("dynamodb", **self.endpoint_url) as ddb_client:
            values = await get_items(ddb_client=ddb_client, keys=keys)
        return {ts_id: rain_mm for ts_id, rain_mm in zip(keys, values)}

    async def has(self, key: TimespanId) -> bool:
        """
        Checks if KeyValueDb has corresponding key.

        Args:
        - key, TimespanId: key to check existence in backend
        Returns:
        - bool: is the key in Db
        """
        async with self.session.client("dynamodb", **self.endpoint_url) as ddb_client:
            result = await has_item(ddb_client=ddb_client, key=key)
        return result

    async def post(self, rains: list[RainStore]) -> None:
        """
        Post new rain values to backend key value db.

        Args:
        - rains, list[RainStore]: rain values to store in backend
        Returns:
        - None
        """
        async with self.session.resource(
            "dynamodb", **self.endpoint_url
        ) as ddb_resource:
            rain_items = {rain.timespan_id: rain.rain_mm for rain in rains}
            await write_items(ddb_resource=ddb_resource, items=rain_items)
        return None
