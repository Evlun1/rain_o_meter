import pytest
from fastapi import HTTPException

from backend.meteofrance.data_gouv_service import get_bulk_file_content


@pytest.mark.anyio
async def test_get_bulk_file_content(mocker, settings, aiohttp_session, mock_responses):
    mocker.patch("backend.meteofrance.data_gouv_service.settings", settings)
    expected_content = bytes("testabcd", "utf8")
    mock_responses.get("www.dgfbulkdata.com", status=200, body=expected_content)

    result = await get_bulk_file_content(session=aiohttp_session)

    assert result == expected_content


@pytest.mark.anyio
async def test_get_bulk_file_content_raise_if_error(
    mocker, settings, aiohttp_session, mock_responses
):
    mocker.patch("backend.meteofrance.data_gouv_service.settings", settings)
    mock_responses.get("www.dgfbulkdata.com", status=500, body="Internal server error")

    with pytest.raises(HTTPException):
        await get_bulk_file_content(session=aiohttp_session)
