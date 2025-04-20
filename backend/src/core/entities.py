from datetime import date
from decimal import Decimal
from typing import Annotated

import pandera.polars as pa
from pydantic import BaseModel, Field

STATION_ID = 75114001  # Montsouris old weather station


TimespanId = Annotated[
    str,
    Field(
        pattern=r"^(M|20\d{2})[0-1]\d[0-3]\d-(M|20\d{2})[0-1]\d[0-3]\d$",
        description=(
            "Timespan identifier in the form of date1-date2. Each date is format %Y%m%d,"
            " with year replaced by 'M' if it's a mean period."
        ),
    ),
]


class RainCompleteInfo(BaseModel):
    last_day: date = Field(description="Last data day available")
    last_day_rain_mm: Decimal = Field(
        ge=0, decimal_places=1, description="Last daily rained amount available"
    )
    month_beg: date = Field(description="Beginning of month day")
    since_month_beg_mm: Decimal = Field(
        ge=0, decimal_places=1, description="Cumulated rain since beginning of month"
    )
    mean_month_beg_mm: Decimal = Field(
        ge=0,
        decimal_places=1,
        description="Mean cumulated rain since beginning of month period",
    )
    prev_30_days: date = Field(description="30 days before last day")
    last_31_days_mm: Decimal = Field(
        ge=0, decimal_places=1, description="Cumulated rain in last 30 days"
    )
    mean_31_days_mm: Decimal = Field(
        ge=0, decimal_places=1, description="Mean cumulated rain in last 30 days period"
    )


class RainStore(BaseModel):
    timespan_id: TimespanId
    rain_mm: Decimal = Field(
        ge=0, decimal_places=1, description="Rained amount for timespan"
    )


class BulkFileSchema(pa.DataFrameModel):
    station_id: int = pa.Field(
        in_range={"min_value": 75e6, "max_value": 76e6}, nullable=False
    )
    date: int = pa.Field(
        in_range={"min_value": 19500101, "max_value": 20250101}, nullable=False
    )
    rainfall_mm: float = pa.Field(ge=0, nullable=True)


class CurrentFileSchema(pa.DataFrameModel):
    date: int = pa.Field(
        in_range={"min_value": 20250101, "max_value": 20990101}, nullable=False
    )
    rainfall_mm: float = pa.Field(ge=0, nullable=False)
