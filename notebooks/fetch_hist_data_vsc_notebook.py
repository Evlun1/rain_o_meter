# %% [markdown]
# ## Get historical rain data
#
# We'll add a bit of quick validation checks with pandera in the middle.
#
# (altough MeteoFrance data is usually quite clean)

# %%
import datetime as dt
import polars as pl
import pandera.polars as pa
import requests

# %%
DOWNLOAD_URL = "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_75_previous-1950-2023_RR-T-Vent.csv.gz"

station_id = 75114001

begin_date = dt.date(1990, 1, 1)
end_date = dt.date(2019, 12, 31)

# %%
bulk_download = requests.get(url=DOWNLOAD_URL)
bulk_download.status_code

# %%
bulk_file_name = "bulk_75.csv.gz"
with open(bulk_file_name, "wb") as bulk_file:
    bulk_file.write(bulk_download.content)

# %%
import polars as pl

bulk_file_df = pl.read_csv(
    bulk_file_name,
    has_header=True,
    columns=["NUM_POSTE", "NOM_USUEL", "AAAAMMJJ", "RR"],
    new_columns=["station_id", "station_name", "date", "rainfall_mm"],
    separator=";",
)
bulk_file_df.head()


# %%
class ValSchema(pa.DataFrameModel):
    station_id: int = pa.Field(
        in_range={"min_value": 75e6, "max_value": 76e6}, nullable=False
    )
    station_name: str
    date: int = pa.Field(
        in_range={"min_value": 19500101, "max_value": 20250101}, nullable=False
    )
    rainfall_mm: float = pa.Field(ge=0, nullable=True)


ValSchema.validate(bulk_file_df)

# %%
prep_df = bulk_file_df.select(
    pl.col("station_id"),
    pl.col("date").cast(pl.String).str.strptime(pl.Date, format="%Y%m%d"),
    pl.col("rainfall_mm"),
)

# %%
interesting_data = prep_df.filter(
    pl.col("station_id") == station_id, pl.col("date").is_between(begin_date, end_date)
).select(
    pl.col("date"),
    pl.col("rainfall_mm"),
    pl.col("date").dt.month().alias("month"),
    pl.col("date").dt.day().alias("day"),
)

interesting_data.head()


# %%
def get_mean_data_between_two_mon_day_dates(df, beg_mon, beg_day, end_mon, end_day):
    year_offset = 1 if (beg_mon, beg_day) > (end_mon, end_day) else 0
    date_beg = dt.date(2020, beg_mon, beg_day)
    date_end = dt.date(2020 + year_offset, end_mon, end_day)
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
    return round(joined_df["rainfall_mm"].sum() / 30, 1)


# %%
get_mean_data_between_two_mon_day_dates(interesting_data, 2, 8, 3, 10)

# %%
get_mean_data_between_two_mon_day_dates(interesting_data, 3, 1, 3, 10)

# %% [markdown]
# It rained less this year than previous years : 45.6mm vs 46.9 on previous sliding month and 2.2mm vs 15.5 since beginning of month.

# %% [markdown]
#
