import datetime as dt

from aiohttp import ClientSession
from async_lru import alru_cache
from cachetools import TTLCache, cached
from fastapi import HTTPException

from settings import get_api_settings

settings = get_api_settings()

ID_STATION = "75114001"  # Paris Montsouris
COMPUTE_DAILY_DATA_ROUTE = "commande-station/quotidienne"
DOWNLOAD_ROUTE = "commande/fichier"


async def get_last_mfapi_data_date() -> dt.date:
    """
    Get latest data date available according to current time.

    According to official documentation on the API, on a given date D, new data
    for D-1 is available after 11:30 UT (where UT ~ UTC).
    We give it a 5min benefit to be sure it is updated correctly.
    https://portail-api.meteofrance.fr/web/fr/DonneesPubliquesClimatologie/documentation  # noqa

    Args:
    - None
    Returns:
    - date: last data date available on DataFile backend
    """
    if settings.fake_last_data_day is None:
        now = dt.datetime.now(dt.timezone.utc)
        delta_days = 2 if now.time() < dt.time(11, 35, 00) else 1
        last_data_date = now.date() - dt.timedelta(days=delta_days)
    else:
        last_data_date = dt.datetime.strptime(
            settings.fake_last_data_day, "%Y-%m-%d"
        ).date()
    return last_data_date


@cached(cache=TTLCache(maxsize=1, ttl=3600))
def get_client_session():
    return ClientSession()


@alru_cache(maxsize=1, ttl=3600)
async def get_mf_access_token(session: ClientSession) -> str:
    """
    Get access token to authentify to Meteo France API.

    Tokens last one hour, and should be fetched through a Depends operation.

    Args:
    - session, ClientSession: aiohttp client session
    Returns:
    - str: token to use, valid one hour
    """
    data = {"grant_type": "client_credentials"}
    headers = {"Authorization": "Basic " + settings.mf_climate_app_id}
    async with session.post(
        url=settings.mf_token_url, data=data, headers=headers, allow_redirects=False
    ) as access_token_response:
        payload = await access_token_response.json()
        token = payload["access_token"]
    return token


async def launch_daily_data_computation(
    session: ClientSession, begin_date: dt.date, token: str
) -> str:
    """
    Launch daily data computation.

    Meteo France backend has an asynchronous data request based on a compute demand
    (this function) and only then a results fetch (following function).

    Args:
    - session, ClientSession: aiohttp client session
    - begin_date, dt.date: date to begin computations from
    - token, str: token to identify this app
    Returns:
    - str: the id of the requested data computation
    """
    begin_time = begin_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = (await get_last_mfapi_data_date()).strftime("%Y-%m-%dT%H:%M:%SZ")

    async with session.get(
        url=f"{settings.mf_climate_app_url}/{COMPUTE_DAILY_DATA_ROUTE}",
        params={
            "id-station": ID_STATION,
            "date-deb-periode": begin_time,
            "date-fin-periode": end_time,
        },
        headers={"Authorization": f"Bearer {token}"},
    ) as launch_computation:
        if (sc := launch_computation.status) // 100 > 2:
            raise HTTPException(status_code=sc, detail=await launch_computation.text())
        payload = await launch_computation.json()

    return payload["elaboreProduitAvecDemandeResponse"]["return"]


async def fetch_daily_data_computation_results(
    session: ClientSession, id_command: str, token: str
) -> str:
    """
    Fetch daily data computation results.

    Args:
    - session, ClientSession: aiohttp client session
    - id_command, str: the id of the requested data computation
    - token, str: token to identify this app
    Returns:
    - str: result CSV file as string
    """
    async with session.get(
        url=f"{settings.mf_climate_app_url}/{DOWNLOAD_ROUTE}",
        params={"id-cmde": id_command},
        headers={"Authorization": f"Bearer {token}"},
    ) as result_computation:
        text = await result_computation.text()
        if (sc := result_computation.status) // 100 > 2:
            raise HTTPException(status_code=sc, detail=text)

    return text
