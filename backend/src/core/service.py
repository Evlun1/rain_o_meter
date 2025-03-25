from datetime import date, timedelta
from pathlib import Path
from core.entities import STATION_ID, RainCompleteInfo, RainStore, TimespanId
from core.exceptions import AlreadyInitialized
from core.protocol import DataFileProtocol, KeyValueDbProtocol

import polars as pl
import pandera as pa


def get_data(
    key_value_db_repo: KeyValueDbProtocol, last_data_day: date
) -> RainCompleteInfo:
    month_beg = date(last_data_day.year, last_data_day.month, 1)
    prev_30_days = last_data_day - timedelta(days=30)
    last_day_tsid: TimespanId = (
        f"{last_data_day.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
    )
    since_month_beg_tsid: TimespanId = (
        f"{month_beg.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
    )
    last_31_days_tsid: TimespanId = (
        f"{prev_30_days.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
    )
    mean_month_beg_tsid: TimespanId = (
        f"M{month_beg.strftime('%m%d')}-M{last_data_day.strftime('%m%d')}"
    )
    mean_31_days_tsid: TimespanId = (
        f"M{prev_30_days.strftime('%m%d')}-M{last_data_day.strftime('%m%d')}"
    )
    rain_data = key_value_db_repo.get(
        keys=[
            last_day_tsid,
            since_month_beg_tsid,
            last_31_days_tsid,
            mean_month_beg_tsid,
            mean_31_days_tsid,
        ]
    )
    return RainCompleteInfo(
        last_day=last_data_day,
        last_day_rain_mm=rain_data[last_day_tsid],
        month_beg=month_beg,
        since_month_beg_mm=rain_data[since_month_beg_tsid],
        mean_month_beg_mm=rain_data[mean_month_beg_tsid],
        prev_30_days=prev_30_days,
        last_31_days_mm=rain_data[last_31_days_tsid],
        mean_31_days_mm=rain_data[mean_31_days_tsid],
    )


def _compute_daily_data(file_path: Path, this_day: date) -> tuple[float, float, float]:
    current_data_df = pl.read_csv(
        file_path,
        has_header=True,
        columns=["DATE", "RR"],
        new_columns=["date", "rainfall_mm"],
        separator=";",
        decimal_comma=True,
    ).select(
        pl.col("date").cast(pl.String).str.strptime(pl.Date, format="%Y%m%d"),
        pl.col("rainfall_mm"),
    )

    this_day_rain = current_data_df.filter(pl.col("date") == this_day)[
        "rainfall_mm"
    ].first()
    last_31_days_rain = current_data_df["rainfall_mm"].sum()
    current_month_beg = date(this_day.year, this_day.month, 1)
    this_month_rain = current_data_df.filter(pl.col("date") >= current_month_beg)[
        "rainfall_mm"
    ].sum()
    return this_day_rain, this_month_rain, last_31_days_rain


def fetch_data_if_not_in_cache(
    key_value_db_repo: KeyValueDbProtocol,
    data_file_repo: DataFileProtocol,
    last_data_day: date,
) -> None:
    last_day_tsid: TimespanId = (
        f"{last_data_day.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
    )

    # If daily data not in cache, compute it
    if not key_value_db_repo.has(last_day_tsid):
        month_beg = date(last_data_day.year, last_data_day.month, 1)
        prev_30_days = last_data_day - timedelta(days=30)
        daily_file_path = data_file_repo.get_daily_file_path(begin_date=prev_30_days)
        last_day_rain_mm, since_month_beg_mm, last_31_days_mm = _compute_daily_data(
            daily_file_path, last_data_day
        )
        since_month_beg_tsid: TimespanId = (
            f"{month_beg.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
        )
        last_31_days_tsid: TimespanId = (
            f"{prev_30_days.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
        )
        key_value_db_repo.post(
            [
                RainStore(timespan_id=last_day_tsid, rain_mm=last_day_rain_mm),
                RainStore(timespan_id=since_month_beg_tsid, rain_mm=since_month_beg_mm),
                RainStore(timespan_id=last_31_days_tsid, rain_mm=last_31_days_mm),
            ]
        )


class BulkFileSchema(pa.DataFrameModel):
    station_id: int = pa.Field(
        in_range={"min_value": 75e6, "max_value": 76e6}, nullable=False
    )
    station_name: str
    date: int = pa.Field(
        in_range={"min_value": 19500101, "max_value": 20250101}, nullable=False
    )
    rainfall_mm: float = pa.Field(ge=0, nullable=True)


def _preprocess_bulk_data(
    df: pl.DataFrame, begin_date: date, end_date: date
) -> pl.DataFrame:
    prep_df = (
        df.select(
            pl.col("station_id"),
            pl.col("date").cast(pl.String).str.strptime(pl.Date, format="%Y%m%d"),
            pl.col("rainfall_mm"),
        )
        .filter(
            pl.col("station_id") == STATION_ID,
            pl.col("date").is_between(begin_date, end_date),
        )
        .select(
            pl.col("date"),
            pl.col("rainfall_mm"),
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.day().alias("day"),
        )
    )
    return prep_df


def _get_mean_data_between_two_mon_day_dates(
    df, beg_mon, beg_day, end_mon, end_day
) -> float:
    year_offset = 1 if (beg_mon, beg_day) > (end_mon, end_day) else 0
    date_beg = date(2020, beg_mon, beg_day)
    date_end = date(2020 + year_offset, end_mon, end_day)
    date_df = (
        pl.date_range(date_beg, date_end, "1d", eager=True)
        .alias("date")
        .to_frame()
        .select(
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.day().alias("day"),
        )
    )
    joined_df = df.join(date_df, on=["month", "day"], how="inner")
    number_of_years = 1 + (df["date"].max().year - df["date"].min().year)
    return round(joined_df["rainfall_mm"].sum() / number_of_years, 1)


def _append_means(df: pl.DataFrame, means: list[RainStore], this_day: date) -> None:
    mean_month_beg_tsid: TimespanId = (
        f"M{this_day.strftime('%m')}01-M{this_day.strftime('%m%d')}"
    )
    prev_30_days = this_day - timedelta(days=30)
    mean_30_days_tsid: TimespanId = (
        f"M{prev_30_days.strftime('%m%d')}-M{this_day.strftime('%m%d')}"
    )
    means.append(
        RainStore(
            timespan_id=mean_month_beg_tsid,
            rain_mm=_get_mean_data_between_two_mon_day_dates(
                df, this_day.month, 1, this_day.month, this_day.day
            ),
        )
    )
    means.append(
        RainStore(
            timespan_id=mean_30_days_tsid,
            rain_mm=_get_mean_data_between_two_mon_day_dates(
                df, prev_30_days.month, prev_30_days.day, this_day.month, this_day.day
            ),
        )
    )


def initialize_mean_data(
    key_value_db_repo: KeyValueDbProtocol,
    data_file_repo: DataFileProtocol,
    year_beg_incl: int,
    year_end_incl: int,
) -> None:
    beginning_tsid: TimespanId = "M0101-M0101"
    if key_value_db_repo.has(beginning_tsid):
        raise AlreadyInitialized

    begin_date = date(year_beg_incl, 1, 1)
    end_date = date(year_end_incl, 12, 31)

    bulk_file_path = data_file_repo.get_bulk_file_path()
    bulk_file_df = pl.read_csv(
        bulk_file_path,
        has_header=True,
        columns=["NUM_POSTE", "NOM_USUEL", "AAAAMMJJ", "RR"],
        new_columns=["station_id", "station_name", "date", "rainfall_mm"],
        separator=";",
    )
    BulkFileSchema.validate(bulk_file_df)
    prep_df = _preprocess_bulk_data(bulk_file_df, begin_date, end_date)

    rain_means: list[RainStore] = []

    for day in pl.date_range(
        date(2000, 1, 1), date(2000, 12, 31), "1d", eager=True
    ).to_list():
        _append_means(prep_df, rain_means, day)

    key_value_db_repo.post(rains=rain_means)
