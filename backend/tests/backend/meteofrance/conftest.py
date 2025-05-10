import pytest
from aiohttp import ClientSession
from aioresponses import aioresponses


@pytest.fixture
async def aiohttp_session(scope="module"):
    async with ClientSession() as session:
        yield session


@pytest.fixture
def mock_responses(scope="module"):
    with aioresponses() as mock:
        yield mock
