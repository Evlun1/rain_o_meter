from datetime import date

from fastapi import FastAPI, Response
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.param_functions import Depends
from fastapi.responses import JSONResponse
from mangum import Mangum

import core.service as core_service
from backend.aws.key_value_db_repository import KeyValueDbRepository
from backend.meteofrance.data_file_repository import DataFileRepository
from core.entities import RainCompleteInfo
from core.exceptions import AlreadyAddedData, AlreadyInitialized
from core.protocol import DataFileProtocol, KeyValueDbProtocol
from settings import get_api_settings

settings = get_api_settings()

app = FastAPI(
    title=settings.api_title,
    description=settings.api_description,
    version=settings.api_version,
)

app_with_middleware = CORSMiddleware(
    app=app,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
handler = Mangum(app_with_middleware)


class AlreadyInitializedHTTPException(HTTPException):
    """Exception raised when the data is already initialized."""

    def __init__(
        self, status_code=409, detail="Backend data already initialized", headers=None
    ):
        super().__init__(status_code, detail, headers)


class AlreadyAddedDataHTTPException(HTTPException):
    """Exception raised when the data is already added to backed."""

    def __init__(
        self, status_code=409, detail="Daily data already added", headers=None
    ):
        super().__init__(status_code, detail, headers)


async def get_last_data_day(
    data_file_repo: DataFileProtocol = Depends(DataFileRepository),
) -> date:
    return await core_service.get_last_data_date(data_file_repo)


@app.get(
    "/",
    response_class=JSONResponse,
    response_model=None,
    description="Get all mandatory data to display in front.",
    status_code=200,  # OK
    responses={
        200: {"description": "Data successfully read"},
    },
)
async def get(
    key_value_db_repo: KeyValueDbProtocol = Depends(KeyValueDbRepository),
    last_data_day: date = Depends(get_last_data_day),
) -> RainCompleteInfo:
    return await core_service.get_data(key_value_db_repo, last_data_day)


@app.get(
    "/add",
    response_class=JSONResponse,
    response_model=None,
    description="Add daily data in backend if needed.",
    status_code=201,  # Created
    responses={
        201: {"description": "Data added in backend"},
        409: {"description": "Data already added in backend"},
    },
)
async def add(
    key_value_db_repo: KeyValueDbProtocol = Depends(KeyValueDbRepository),
    data_file_repo: DataFileProtocol = Depends(DataFileRepository),
    last_data_day: date = Depends(get_last_data_day),
):
    try:
        await core_service.fetch_daily_data_if_not_in_cache(
            key_value_db_repo, data_file_repo, last_data_day
        )
    except AlreadyAddedData as exc:
        raise AlreadyAddedDataHTTPException(detail=exc.message)
    return Response(status_code=201)


@app.get(
    "/initialize",
    response_class=JSONResponse,
    response_model=None,
    description="Initialize backend data.",
    status_code=201,  # Created
    responses={
        201: {"description": "Backend initialized."},
        409: {"description": "Backend already initialized."},
    },
)
async def initialize(
    key_value_db_repo: KeyValueDbProtocol = Depends(KeyValueDbRepository),
    data_file_repo: DataFileProtocol = Depends(DataFileRepository),
):
    try:
        await core_service.initialize_mean_data(
            key_value_db_repo,
            data_file_repo,
            settings.year_beg_incl,
            settings.year_end_incl,
        )
    except AlreadyInitialized as exc:
        raise AlreadyInitializedHTTPException(detail=exc.message)
    return Response(status_code=201)
