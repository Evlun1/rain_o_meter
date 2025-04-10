from functools import cache
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from mypy_boto3_dynamodb.type_defs import (
    BatchGetItemInputTypeDef,
    BatchGetItemOutputTypeDef,
    GetItemOutputTypeDef,
)
import boto3
from settings import get_api_settings

settings = get_api_settings()


@cache
def get_aws_client() -> DynamoDBClient:
    return boto3.client("dynamodb")


@cache
def get_aws_resource() -> DynamoDBServiceResource:
    return boto3.resource("dynamodb")


def get_items(ddb_client: DynamoDBClient, keys: list[str]) -> list[float]:
    """
    Get rain items from DDB.

    Note that batch_get_item need item handling after 100 items gotten but this is not
    an expected use case.

    Args:
    - ddb_client, DynamoDbClient: boto client
    - keys, list[str] : list of keys to request table
    Returns:
    - list[float] : list of rain amounts for given timestamps
    """
    request_items: BatchGetItemInputTypeDef = {
        settings.backend_table_name: {
            "Keys": [{settings.backend_table_key_name: {"S": k}} for k in keys]
        }
    }
    raw_result: BatchGetItemOutputTypeDef = ddb_client.batch_get_item(
        RequestItems=request_items
    )
    return [
        float(rain_res[settings.backend_table_value_name]["N"])
        for rain_res in raw_result["Responses"][settings.backend_table_name]
    ]


def write_items(ddb_resource: DynamoDBServiceResource, items: dict[str, float]) -> None:
    """
    Write given items in DDB table.

    Args:
    - ddb_resource, DynamoDBServiceResource: boto resource
    - items, dict[str, float] : key-value data to store
    Returns:
    - None
    """
    ddb_table = ddb_resource.Table(settings.backend_table_name)
    with ddb_table.batch_writer() as writer:
        for k, v in items.items():
            writer.put_item(
                Item={
                    settings.backend_table_key_name: k,
                    settings.backend_table_value_name: str(v),
                }
            )


def has_item(ddb_client: DynamoDBClient, key: str) -> bool:
    """
    Checks if given key is in backend table.

    Args:
    - ddb_client, DynamoDBClient: boto client
    - key, str: key to check existence
    Returns:
    - bool: is the key in backend table
    """
    response: GetItemOutputTypeDef = ddb_client.get_item(
        TableName=settings.backend_table_name,
        Key={settings.backend_table_key_name: {"S": key}},
    )
    return "Item" in response
