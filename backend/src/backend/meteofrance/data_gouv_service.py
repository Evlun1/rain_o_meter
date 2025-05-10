from aiohttp import ClientSession
from fastapi import HTTPException

from settings import get_api_settings

settings = get_api_settings()


async def get_bulk_file_content(session: ClientSession) -> bytes:
    """
    Get bulk file content and return it.

    As an improvement, it could be worth to stream the contents and write them
    as chunked data rather than load the whole thing in memory.
    https://docs.aiohttp.org/en/stable/client_quickstart.html#streaming-response-content

    That said, file is around 4MB zipped, and has to be handled only once.
    Things should be ok in a 128MB lambda.

    Args:
    - session, ClientSession: aiohttp session
    Returns:
    - bytes: file content to later write to memory
    """
    async with session.get(url=settings.dgf_historical_data_url) as download:
        if (sc := download.status) // 100 > 2:
            text = await download.text()
            raise HTTPException(status_code=sc, detail=text)
        content = await download.read()

    return content
