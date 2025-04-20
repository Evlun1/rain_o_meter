from typing import Self

from fastapi.param_functions import Depends
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource

from backend.aws.dynamodb_service import (
    get_aws_client,
    get_aws_resource,
    get_items,
    has_item,
    write_items,
)
from core.entities import RainStore, TimespanId


class KeyValueDbRepository:
    ddb_client: DynamoDBClient
    ddb_resource: DynamoDBServiceResource

    def __init__(
        self,
        ddb_client: DynamoDBClient = Depends(get_aws_client),
        ddb_resource: DynamoDBServiceResource = Depends(get_aws_resource),
    ) -> Self:
        self.ddb_client = ddb_client
        self.ddb_resource = ddb_resource

    def get(self, keys: list[TimespanId]) -> dict[TimespanId, float]:
        """
        Get values corresponding to keys of KeyValueDb.

        We assume values exist in Db.

        Args:
        - keys, list[TimespanId]: keys to get values
        Returns:
        - dict[TimespanId, float]: dict with input keys & corresponding values
        """
        values = get_items(ddb_client=self.ddb_client, keys=keys)
        return {ts_id: rain_mm for ts_id, rain_mm in zip(keys, values)}

    def has(self, key: TimespanId) -> bool:
        """
        Checks if KeyValueDb has corresponding key.

        Args:
        - key, TimespanId: key to check existence in backend
        Returns:
        - bool: is the key in Db
        """
        return has_item(ddb_client=self.ddb_client, key=key)

    def post(self, rains: list[RainStore]) -> None:
        """
        Post new rain values to backend key value db.

        Args:
        - rains, list[RainStore]: rain values to store in backend
        Returns:
        - None
        """
        rain_items = {rain.timespan_id: rain.rain_mm for rain in rains}
        return write_items(ddb_resource=self.ddb_resource, items=rain_items)
