from functools import cache

from aioboto3 import Session
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from mypy_boto3_dynamodb.type_defs import (
    BatchGetItemInputTypeDef,
    BatchGetItemOutputTypeDef,
    GetItemOutputTypeDef,
)

from settings import get_api_settings

settings = get_api_settings()


@cache
def get_aws_session() -> Session:
    return Session()


async def get_items(ddb_client: DynamoDBClient, keys: list[str]) -> list[float]:
    """
    Get rain items from DDB.

    Note that batch_get_item need item handling after 100 items gotten but this is not
    an expected use case.

    Args:
    - ddb_client, DynamoDBClient: aioboto3 dynamodb client
    - keys, list[str] : list of keys to request table
    Returns:
    - list[float] : list of rain amounts for given timestamps
    """
    request_items: BatchGetItemInputTypeDef = {
        settings.backend_table_name: {
            "Keys": [{settings.backend_table_key_name: {"S": k}} for k in keys]
        }
    }
    raw_result: BatchGetItemOutputTypeDef = await ddb_client.batch_get_item(
        RequestItems=request_items
    )
    return [
        float(rain_res[settings.backend_table_value_name]["N"])
        for rain_res in raw_result["Responses"][settings.backend_table_name]
    ]


async def write_items(
    ddb_resource: DynamoDBServiceResource, items: dict[str, float]
) -> None:
    """
    Write given items in DDB table.

    Args:
    - ddb_resource, DynamoDBServiceResource: aioboto3 dynamodb resource
    - items, dict[str, float] : key-value data to store
    Returns:
    - None
    """
    ddb_table = await ddb_resource.Table(settings.backend_table_name)
    async with ddb_table.batch_writer() as writer:
        for k, v in items.items():
            await writer.put_item(
                Item={
                    settings.backend_table_key_name: k,
                    settings.backend_table_value_name: v,
                }
            )


async def has_item(ddb_client: DynamoDBClient, key: str) -> bool:
    """
    Checks if given key is in backend table.

    Args:
    - ddb_client, DynamoDBClient: aioboto3 dynamodb client
    - key, str: key to check existence
    Returns:
    - bool: is the key in backend table
    """
    response: GetItemOutputTypeDef = await ddb_client.get_item(
        TableName=settings.backend_table_name,
        Key={settings.backend_table_key_name: {"S": key}},
    )
    return "Item" in response
