# Rain O Meter backend

This is the backbone of the global application relying on FastAPI python package.

It features three routes :
- GET /day_data : to be called by front end to fetch daily info (yesterday rain, past month rain and past data averages)
- GET /add : add latest data from MeteoFrance API to cache (DynamoDb). _Called once per day through an event rule when deployed._
- GET /initialize : initialize average data from data.gouv.fr MeteoFrance history data to cache (DynamoDb). _Called once on deployment through Terraform._

## Clean code practises

- a package & venv manager (_uv_)
- using code formatter and linter (_ruff_)
- 100% coverage unit tests (_pytest_)
- types, docstrings and comments
- an onion architecture, with separate data models, application code and backends code
- a fully aysnchronous FastAPI application (_async code, aioboto3, aiohttp_)
- data validation packages (_pydantic for records, pandera for dataframes_)
- local integration tests (_docker, docker-compose_)
- and more, have a look :)

Envisioned (but not added yet) clean code practises include :
- add a CI for quality and tests
- add a type checker such as mypy
- use Polars LazyFrame instead of DataFrame for a little speedup
- add commitizen to bump versions and format commits

## Setup

### Requirements

Mandatory :
- [uv package manager](https://docs.astral.sh/uv/getting-started/installation/) _(note that you can install required python 3.13 venv through uv)_

Optional :
- [docker & docker compose](https://docs.docker.com/compose/install/) for local integration tests
- [dynamodb-admin](https://github.com/aaronshaf/dynamodb-admin) to browse local dynamodb contents (setup through docker compose)
- [a portail-api MétéoFrance account](https://portail-api.meteofrance.fr/web/fr) with a subscription to "Données Climatologiques" API to query real data
- an AWS account, to test app locally with a deployed dynamodb

### Use application

All following commands are run in a terminal (assuming Linux-based but could work in PowerShell).

Install virtual environment & package dependencies :
```bash
cd backend
uv venv
uv sync
```

Run code formatiing, code quality & code test checks :
```bash
uv run task quality
uv run task tests
```

Launch app locally :
```bash
cp .env.dist .env
[complete .env file values, add your MétéoFrance API token]
[if you want to interact with AWS backend, setup your terminal appropriately]
uv run task dev
```

Launch app locally with local dynamodb backend and mocked MeteoFrance / DataGouvFr calls :
```
docker compose up --build -d
uv run task ddb_admin [optional, to browse local ddb contents]
```