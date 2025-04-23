import pytest

from backend.aws.dynamodb_service import (
    get_items,
    has_item,
    write_items,
)


@pytest.mark.anyio
async def test_get_items(event_loop, mocker, settings, dynamodb_client):
    # Setup mock data
    mocker.patch("backend.aws.dynamodb_service.settings", settings)
    await dynamodb_client.create_table(
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
        await dynamodb_client.put_item(
            TableName=settings.backend_table_name,
            Item={
                settings.backend_table_key_name: {"S": f"key{str(k)}"},
                settings.backend_table_value_name: {"N": f"{str(k)}.0"},
            },
        )

    keys = ["key1", "key4"]
    result = await get_items(dynamodb_client, keys)
    assert result == [1, 4]
    await dynamodb_client.delete_table(TableName=settings.backend_table_name)


@pytest.mark.anyio
async def test_write_items(
    event_loop, mocker, settings, dynamodb_resource, dynamodb_client
):
    mocker.patch("backend.aws.dynamodb_service.settings", settings)
    await dynamodb_resource.create_table(
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
    await write_items(dynamodb_resource, items)

    table = await dynamodb_resource.Table(settings.backend_table_name)
    response = await table.get_item(Key={settings.backend_table_key_name: "key1"})
    assert response["Item"][settings.backend_table_value_name] == "1"
    await dynamodb_client.delete_table(TableName=settings.backend_table_name)


@pytest.mark.anyio
async def test_has_item(event_loop, mocker, settings, dynamodb_client):
    mocker.patch("backend.aws.dynamodb_service.settings", settings)
    await dynamodb_client.create_table(
        TableName=settings.backend_table_name,
        KeySchema=[
            {"AttributeName": settings.backend_table_key_name, "KeyType": "HASH"}
        ],
        AttributeDefinitions=[
            {"AttributeName": settings.backend_table_key_name, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    await dynamodb_client.put_item(
        TableName=settings.backend_table_name,
        Item={
            settings.backend_table_key_name: {"S": "key1"},
            settings.backend_table_value_name: {"N": "1.0"},
        },
    )

    assert await has_item(dynamodb_client, "key1") is True
    assert await has_item(dynamodb_client, "key2") is False
    await dynamodb_client.delete_table(TableName=settings.backend_table_name)
