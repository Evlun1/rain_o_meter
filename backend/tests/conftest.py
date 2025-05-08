import pytest

from settings import Settings


@pytest.fixture()
def mock_module(mocker):
    return lambda name, spec: mocker.Mock(name=name, spec=spec)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def settings():
    return Settings(
        _env_file=".env",
        year_beg_incl=2020,
        year_end_incl=2021,
        backend_table_name="test_table",
        backend_table_key_name="test_key",
        backend_table_value_name="test_value",
        mf_climate_app_id="1234ab",
        mf_token_url="www.testtoken.com",
        mf_climate_app_url="www.mfapp.com",
        dgf_historical_data_url="www.dgfbulkdata.com",
    )


@pytest.fixture
def settings_with_fake_date():
    return Settings(
        _env_file=".env",
        year_beg_incl=2020,
        year_end_incl=2021,
        backend_table_name="test_table",
        backend_table_key_name="test_key",
        backend_table_value_name="test_value",
        mf_climate_app_id="1234ab",
        mf_token_url="www.testtoken.com",
        mf_climate_app_url="www.mfapp.com",
        dgf_historical_data_url="www.dgfbulkdata.com",
        fake_last_data_day="2025-04-01",
    )
