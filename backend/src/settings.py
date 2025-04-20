from enum import StrEnum, auto
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    LOCAL = auto()
    DEPLOYED = auto()


CORS_ORIGINS = {
    Environment.LOCAL: ["*"],
    Environment.DEPLOYED: ["*"],  # Todo : once deployed, replace with front url
}


class Settings(BaseSettings):
    environment: Environment
    api_title: str = "Rain O Meter API"
    api_description: str = "Expose Meteo France data as a rain meter."
    api_version: str = "0.1.0"
    year_beg_incl: int
    year_end_incl: int
    backend_table_name: str = "rainfall"
    backend_table_key_name: str = "timestamp_id"
    backend_table_value_name: str = "rain_mm"
    mf_token_url: str = "https://portail-api.meteofrance.fr/token"
    mf_climate_app_id: str
    mf_climate_app_url: str = "https://public-api.meteofrance.fr/public/DPClim/v1"
    dgf_historical_data_url: str = "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_75_previous-1950-2023_RR-T-Vent.csv.gz"  # noqa

    @property
    def cors_origins(self) -> list[str]:
        return CORS_ORIGINS[self.environment]

    model_config = SettingsConfigDict(env_file=".env")


@lru_cache()
def get_api_settings() -> Settings:
    return Settings(_env_file=".env")  # type: ignore
