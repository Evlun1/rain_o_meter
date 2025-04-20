import pytest

from settings import Settings


@pytest.fixture
def settings():
    return Settings(
        _env_file=".env",
        backend_table_name="test_table",
        backend_table_key_name="test_key",
        backend_table_value_name="test_value",
        mf_climate_app_id="1234ab",
        mf_token_url="www.testtoken.com",
        mf_climate_app_url="www.mfapp.com",
        dgf_historical_data_url="www.dgfbulkdata.com",
    )
