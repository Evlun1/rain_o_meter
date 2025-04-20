from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource

from backend.aws.dynamodb_service import (
    get_items,
    has_item,
    write_items,
)


def test_get_items(mocker, ddb_client: DynamoDBClient, settings):
    # Setup mock data
    mocker.patch("backend.aws.dynamodb_service.settings", settings)
    ddb_client.create_table(
        TableName=settings.backend_table_name,
        KeySchema=[
            {"AttributeName": settings.backend_table_key_name, "KeyType": "HASH"}
        ],
        AttributeDefinitions=[
            {"AttributeName": settings.backend_table_key_name, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    for k in range(5):
        ddb_client.put_item(
            TableName=settings.backend_table_name,
            Item={
                settings.backend_table_key_name: {"S": f"key{str(k)}"},
                settings.backend_table_value_name: {"N": f"{str(k)}.0"},
            },
        )

    keys = ["key1", "key4"]
    result = get_items(ddb_client, keys)
    assert result == [1, 4]


def test_write_items(ddb_resource: DynamoDBServiceResource, mocker, settings):
    mocker.patch("backend.aws.dynamodb_service.settings", settings)
    ddb_resource.create_table(
        TableName=settings.backend_table_name,
        KeySchema=[
            {"AttributeName": settings.backend_table_key_name, "KeyType": "HASH"}
        ],
        AttributeDefinitions=[
            {"AttributeName": settings.backend_table_key_name, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    items = {"key1": 1, "key2": 2}
    write_items(ddb_resource, items)

    table = ddb_resource.Table(settings.backend_table_name)
    response = table.get_item(Key={settings.backend_table_key_name: "key1"})
    assert response["Item"][settings.backend_table_value_name] == "1"


def test_has_item(ddb_client: DynamoDBClient, mocker, settings):
    mocker.patch("backend.aws.dynamodb_service.settings", settings)
    ddb_client.create_table(
        TableName=settings.backend_table_name,
        KeySchema=[
            {"AttributeName": settings.backend_table_key_name, "KeyType": "HASH"}
        ],
        AttributeDefinitions=[
            {"AttributeName": settings.backend_table_key_name, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    ddb_client.put_item(
        TableName=settings.backend_table_name,
        Item={
            settings.backend_table_key_name: {"S": "key1"},
            settings.backend_table_value_name: {"N": "1.0"},
        },
    )

    assert has_item(ddb_client, "key1") is True
    assert has_item(ddb_client, "key2") is False
