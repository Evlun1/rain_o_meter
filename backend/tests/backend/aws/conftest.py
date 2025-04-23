import shutil
import signal
import subprocess as sp
import time
from typing import Dict

import pytest
import requests
from aioboto3.session import Session
from aiobotocore.config import AioConfig

from backend.aws.dynamodb_service import (
    get_aws_session,
)


@pytest.fixture(scope="module")
def aws_session():
    yield get_aws_session()


"""
Okay so the idea concerning lengthy stuff below is the following :
1. Moto is an incredible tool to test AWS related stuff
2. But it doesn't work with aioboto3/core because it is sync in a fixed thread

Conclusion : writing aioboto is fun, testing aioboto quickly turns to a nightmare

Below code does the following :
- define a moto server as a subprocess
- launch a moto server as a fixture
- inject moto server config in client & resources objects

The code was designed with moto4 in mind, which required separate servers for separate
services. This probably changed in moto5, but it doesn't matter as all we're doing here
are calls to dynamodb.

All credits goes to aioboto3 main developer Terri Cain :
- https://github.com/terricain/aioboto3/blob/main/tests/mock_server.py
- https://github.com/terricain/aioboto3/blob/main/tests/conftest.py
"""

_proxy_bypass = {
    "http": None,
    "https": None,
}


def start_service(service_name, host, port):
    moto_svr_path = shutil.which("moto_server")
    args = [moto_svr_path, "-H", host, "-p", str(port)]
    process = sp.Popen(
        args, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE
    )  # shell=True
    url = "http://{host}:{port}".format(host=host, port=port)

    for i in range(0, 30):
        output = process.poll()
        if output is not None:
            print("moto_server exited status {0}".format(output))
            stdout, stderr = process.communicate()
            print("moto_server stdout: {0}".format(stdout))
            print("moto_server stderr: {0}".format(stderr))
            pytest.fail("Can not start service: {}".format(service_name))

        try:
            # we need to bypass the proxies due to monkeypatches
            requests.get(url, timeout=5, proxies=_proxy_bypass)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)
    else:
        stop_process(process)  # pytest.fail doesn't call stop_process
        pytest.fail("Can not start service: {}".format(service_name))

    return process


def stop_process(process):
    try:
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=20)
    except sp.TimeoutExpired:
        process.kill()
        outs, errors = process.communicate(timeout=20)
        exit_code = process.returncode
        msg = "Child process finished {} not in clean way: {} {}".format(
            exit_code, outs, errors
        )
        raise RuntimeError(msg)


@pytest.fixture(scope="session")
def dynamodb_server():
    host = "localhost"
    port = 5001
    url = "http://{host}:{port}".format(host=host, port=port)
    process = start_service("dynamodb", host, port)
    yield url
    stop_process(process)


def moto_config() -> Dict[str, str]:
    return {"aws_secret_access_key": "xxx", "aws_access_key_id": "xxx"}


@pytest.fixture
def region() -> str:
    return "eu-central-1"


@pytest.fixture
def signature_version() -> str:
    return "v4"


@pytest.fixture
def config(signature_version: str) -> AioConfig:
    return AioConfig(
        signature_version=signature_version, read_timeout=5, connect_timeout=5
    )


@pytest.fixture
async def dynamodb_resource(
    region: str, config: AioConfig, event_loop, dynamodb_server: str
):
    session = Session(region_name=region, **moto_config())

    async with session.resource(
        "dynamodb", region_name=region, endpoint_url=dynamodb_server, config=config
    ) as resource:
        yield resource


@pytest.fixture
async def dynamodb_client(
    region: str, config: AioConfig, event_loop, dynamodb_server: str
):
    session = Session(region_name=region, **moto_config())

    async with session.client(
        "dynamodb", region_name=region, endpoint_url=dynamodb_server, config=config
    ) as client:
        yield client
