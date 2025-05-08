from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from core.entities import (
    STATION_ID,
    BulkFileSchema,
    CurrentFileSchema,
    RainCompleteInfo,
    RainStore,
    TimespanId,
)
from core.exceptions import AlreadyAddedData, AlreadyInitialized
from core.protocol import DataFileProtocol, KeyValueDbProtocol


async def get_data(
    key_value_db_repo: KeyValueDbProtocol, last_data_day: date
) -> RainCompleteInfo:
    """
    Get all data useful for front display. This assumes all data is already cached.

    Args :
    - key_value_db_repo : cache db backend repository
    - last_data_day, date : last known date to fetch data for
    Returns :
    - RainCompleteInfo : object with all info for frontend
    """
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
    rain_data = await key_value_db_repo.get(
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


async def _compute_daily_data(
    file_path: Path, this_day: date
) -> tuple[float, float, float]:
    """
    Extracts all daily data : this day rain, this month rain and last 31 days rain.

    Args :
    - file_path, Path : path to read CSV file with daily data
    - this_day, date : day to consider data from
    Returns :
    - float : this day rain
    - float : this month rain
    - float : last 31 days cumulated rain
    """
    current_data_df = pl.read_csv(
        file_path,
        has_header=True,
        columns=["DATE", "RR"],
        new_columns=["date", "rainfall_mm"],
        separator=";",
        decimal_comma=True,
    )
    CurrentFileSchema.validate(current_data_df)
    current_data_df = current_data_df.select(
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


async def fetch_daily_data_if_not_in_cache(
    key_value_db_repo: KeyValueDbProtocol,
    data_file_repo: DataFileProtocol,
    last_data_day: date,
) -> None:
    """
    Checks if data for last_day is in cache, and if not, fetch it and store it.

    Args :
    - key_value_db_repo : cache db backend repository
    - data_file_repo : download data backend repository
    - last_data_day, date : last known date to check data for
    Returns :
    - None
    """
    last_day_tsid: TimespanId = (
        f"{last_data_day.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
    )

    # If daily data not in cache, compute it
    if await key_value_db_repo.has(last_day_tsid):
        raise AlreadyAddedData

    month_beg = date(last_data_day.year, last_data_day.month, 1)
    prev_30_days = last_data_day - timedelta(days=30)
    async with data_file_repo.get_daily_file_path(
        begin_date=prev_30_days
    ) as daily_file_path:
        (
            last_day_rain_mm,
            since_month_beg_mm,
            last_31_days_mm,
        ) = await _compute_daily_data(daily_file_path, last_data_day)
    since_month_beg_tsid: TimespanId = (
        f"{month_beg.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
    )
    last_31_days_tsid: TimespanId = (
        f"{prev_30_days.strftime('%Y%m%d')}-{last_data_day.strftime('%Y%m%d')}"
    )
    await key_value_db_repo.post(
        [
            RainStore(timespan_id=last_day_tsid, rain_mm=last_day_rain_mm),
            RainStore(timespan_id=since_month_beg_tsid, rain_mm=since_month_beg_mm),
            RainStore(timespan_id=last_31_days_tsid, rain_mm=last_31_days_mm),
        ]
    )


async def _preprocess_bulk_data(
    df: pl.DataFrame, begin_date: date, end_date: date
) -> pl.DataFrame:
    """
    Preprocess raw bulk data file. Bit of filtering and renaming.

    Args :
    - df, pl.DataFrame : bulk data df
    - begin_date, date : date to keep measurements from
    - end_date, date : date to keep measurements until
    Returns :
    - pl.DataFrame : for selected station_id only and between begin and end dates
    """
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


async def _get_mean_data_between_two_mon_day_dates(
    df: pl.DataFrame,
    beg_mon: int,
    beg_day: int,
    end_mon: int,
    end_day: int,
    number_of_years: int,
) -> float:
    """
    Compute mean cumulated rain data on df between two dates.

    Dates are in month and day only as we average it on multiple years.

    Args :
    - df, pl.DataFrame : df with data to average
    - beg_mon, int : month of period to begin average
    - beg_day, int : day of period to begin average
    - end_mon, int : month of period to end average
    - end_day, int : day of period to end average
    - number_of_years, int : number of years of data in df
    Returns :
    - float : Mean rainfall on considered period, at tenth of mm
    """
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
    return round(joined_df["rainfall_mm"].sum() / number_of_years, 1)


async def _compute_history_means(
    df: pl.DataFrame, this_day: date, number_of_years: int
) -> tuple[float, float]:
    """
    Compute history averages since beginning of month and last 31 days.

    This works with dataframes with complete year data, otherwise there can
    be some side effects for periods between years.

    Args :
    - df, pl.DataFrame: df with data to average
    - this_day, date : end day for both periods
    - number_of_years, int : number of years in df
    Returns :
    - float : average on beg_month-this_day period
    - float : average on prev_30_days-this_day period
    """
    mean_month_beg_tsid: TimespanId = (
        f"M{this_day.strftime('%m')}01-M{this_day.strftime('%m%d')}"
    )
    prev_30_days = this_day - timedelta(days=30)
    mean_30_days_tsid: TimespanId = (
        f"M{prev_30_days.strftime('%m%d')}-M{this_day.strftime('%m%d')}"
    )
    means = [
        RainStore(
            timespan_id=mean_month_beg_tsid,
            rain_mm=await _get_mean_data_between_two_mon_day_dates(
                df, this_day.month, 1, this_day.month, this_day.day, number_of_years
            ),
        ),
        RainStore(
            timespan_id=mean_30_days_tsid,
            rain_mm=await _get_mean_data_between_two_mon_day_dates(
                df,
                prev_30_days.month,
                prev_30_days.day,
                this_day.month,
                this_day.day,
                number_of_years,
            ),
        ),
    ]

    # Handle leap year case messing "30 previous days" rolling period
    this_year = this_day.year
    if (
        (this_year % 4 == 0 and this_year % 100 != 0) or (this_year % 400 == 0)
    ) and (  # leap year
        prev_30_days <= date(this_year, 2, 29) < this_day
    ):
        prev_31_days = prev_30_days - timedelta(days=1)
        mean_31_days_tsid: TimespanId = (
            f"M{prev_31_days.strftime('%m%d')}-M{this_day.strftime('%m%d')}"
        )
        means.append(
            RainStore(
                timespan_id=mean_31_days_tsid,
                rain_mm=await _get_mean_data_between_two_mon_day_dates(
                    df,
                    prev_31_days.month,
                    prev_31_days.day,
                    this_day.month,
                    this_day.day,
                    number_of_years,
                ),
            )
        )

    return means


async def initialize_mean_data(
    key_value_db_repo: KeyValueDbProtocol,
    data_file_repo: DataFileProtocol,
    year_beg_incl: int,
    year_end_incl: int,
) -> None:
    """
    Initialize mean data : fetch history file, compute means and store them.

    Args :
    - key_value_db_repo : cache db backend repository
    - data_file_repo : download data backend repository
    - year_beg_incl, int : year to begin averaging data from (included)
    - year_end_incl, int : year to end averaging data until (INCLUDED)
    Returns :
    - none
    """
    beginning_tsid: TimespanId = "M0101-M0101"
    if await key_value_db_repo.has(beginning_tsid):
        raise AlreadyInitialized

    begin_date = date(year_beg_incl, 1, 1)
    end_date = date(year_end_incl, 12, 31)

    async with data_file_repo.get_bulk_file_path() as bulk_file_path:
        bulk_file_df = pl.read_csv(
            bulk_file_path,
            has_header=True,
            columns=["NUM_POSTE", "AAAAMMJJ", "RR"],
            new_columns=["station_id", "date", "rainfall_mm"],
            separator=";",
        )
    BulkFileSchema.validate(bulk_file_df)
    prep_df = await _preprocess_bulk_data(bulk_file_df, begin_date, end_date)

    rain_means: list[RainStore] = []
    number_of_years = 1 + (prep_df["date"].max().year - prep_df["date"].min().year)

    for day in pl.date_range(
        date(2000, 1, 1), date(2000, 12, 31), "1d", eager=True
    ).to_list():
        day_means = await _compute_history_means(prep_df, day, number_of_years)
        rain_means.extend(deepcopy(day_means))  # need deepcopy because of pydantic obj

    await key_value_db_repo.post(rains=rain_means)


async def get_last_data_date(data_file_repo: DataFileProtocol) -> date:
    return await data_file_repo.get_last_data_date()
