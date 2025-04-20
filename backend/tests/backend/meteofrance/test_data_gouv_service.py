from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from requests.models import Response

from backend.meteofrance.data_gouv_service import get_bulk_file_content


def test_get_bulk_file_content(mocker, settings):
    mocker.patch("backend.meteofrance.data_gouv_service.settings", settings)
    expected_content = bytes("testabcd", "utf8")
    response = Mock(spec=Response)
    response.status_code = 200
    response.content = expected_content
    get_patch = mocker.patch(
        "backend.meteofrance.data_gouv_service.rq.get",
        return_value=response,
    )

    result = get_bulk_file_content()

    get_patch.assert_called_once_with(url="www.dgfbulkdata.com")
    assert result == expected_content


def test_get_bulk_file_content_raise_if_error(mocker, settings):
    mocker.patch("backend.meteofrance.data_gouv_service.settings", settings)
    response = Mock(spec=Response)
    response.status_code = 500
    response.text = "Internal server error"
    mocker.patch(
        "backend.meteofrance.data_gouv_service.rq.get",
        return_value=response,
    )

    with pytest.raises(HTTPException):
        get_bulk_file_content()
