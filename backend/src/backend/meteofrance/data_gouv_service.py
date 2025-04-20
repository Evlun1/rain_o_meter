import requests as rq
from fastapi import HTTPException

from settings import get_api_settings

settings = get_api_settings()


def get_bulk_file_content() -> bytes:
    download = rq.get(url=settings.dgf_historical_data_url)

    if (sc := download.status_code) // 100 > 2:
        raise HTTPException(status_code=sc, detail=download.text)

    return download.content
