# %% [markdown]
# ## Authorization stuff
#
# ### Application ID
#
# Go to my account on MF, subscribe to appropriate API, then generate token, and have a look at application_id in Curl command
#
# ### Get token
#
# Usual Oauth2 procedure, have a look at https://portail-api.meteofrance.fr/web/fr/faq for more info.

# %%
APPLICATION_ID = (
    "eDYyNUVvQXNQVGxORGR5R1lsMVdENENBR0JnYTp2RW5iQVZfRUhpZGQzeWliYnpCaDJVUFJHTXdh"
)
TOKEN_URL = "https://portail-api.meteofrance.fr/token"

# %%
import requests

data = {"grant_type": "client_credentials"}
headers = {"Authorization": "Basic " + APPLICATION_ID}
access_token_response = requests.post(
    TOKEN_URL, data=data, allow_redirects=False, headers=headers
)
token = access_token_response.json()["access_token"]
token

# %% [markdown]
# ## Get station info

# %%
CLIM_API_URL = "https://public-api.meteofrance.fr/public/DPClim/v1"
LISTE_STATIONS_ROUTE = "liste-stations/quotidienne"
INFO_STATIONS_ROUTE = "information-station"

# %%
stations_75 = requests.get(
    url=f"{CLIM_API_URL}/{LISTE_STATIONS_ROUTE}",
    params={"id-departement": 75, "parametre": "precipitation"},
    headers={"Authorization": f"Bearer {token}"},
)
stations_75.status_code

# %%
print("\n".join(stations_75.text.split("}, {")))

# %% [markdown]
# Let's keep Montsouris for this.
#
# ```
# "id": "75114001", "nom": "PARIS-MONTSOURIS", "posteOuvert": true, "typePoste": 0, "lon": 2.337833, "lat": 48.821667, "alt": 75, "postePublic": true
# ```

# %%
ms_station_info = requests.get(
    url=f"{CLIM_API_URL}/{INFO_STATIONS_ROUTE}",
    params={"id-station": "75114001"},
    headers={"Authorization": f"Bearer {token}"},
)
ms_station_info.status_code

# %%
print("\n".join(map(str, ms_station_info.json()[0]["parametres"])))

# %% [markdown]
# Okay we have rain here since 1873 (!), which should be more than enough for our use case.
#
# ```
# {'nom': 'HAUTEUR DE PRECIPITATIONS QUOTIDIENNE', 'dateDebut': '1873-01-01 00:00:00', 'dateFin': ''}
# ```
#
# ## Get rain data

# %%
COMPUTE_DAILY_DATA_ROUTE = "commande-station/quotidienne"
DOWNLOAD_ROUTE = "commande/fichier"

# %%
import datetime as dt


# %%
begin_time = (dt.date.today() - dt.timedelta(days=31)).strftime("%Y-%m-%dT%H:%M:%SZ")
end_time = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

launch_computation = requests.get(
    url=f"{CLIM_API_URL}/{COMPUTE_DAILY_DATA_ROUTE}",
    params={
        "id-station": "75114001",
        "date-deb-periode": begin_time,
        "date-fin-periode": end_time,
    },
    headers={"Authorization": f"Bearer {token}"},
)
launch_computation.status_code

# %%
launch_computation.text

# %%
result = requests.get(
    url=f"{CLIM_API_URL}/{DOWNLOAD_ROUTE}",
    params={
        "id-cmde": launch_computation.json()["elaboreProduitAvecDemandeResponse"][
            "return"
        ]
    },
    headers={"Authorization": f"Bearer {token}"},
)
result.status_code

# %%
result.text

# %%
current_data_file = "test_current_data.csv"

with open(current_data_file, "w") as current_csv_file:
    current_csv_file.write(result.text)

# %%
import polars as pl

current_data_df = pl.read_csv(
    current_data_file,
    has_header=True,
    columns=["DATE", "RR"],
    new_columns=["date", "rainfall_mm"],
    separator=";",
    decimal_comma=True,
)
current_data_df.head()

# %%
current_data_df = current_data_df.select(
    pl.col("date").cast(pl.String).str.strptime(pl.Date, format="%Y%m%d"),
    pl.col("rainfall_mm"),
)
current_data_df.head()

# %%
yesterday = dt.date.today() - dt.timedelta(days=1)
yesterday_rain = current_data_df.filter(pl.col("date") == yesterday)[
    "rainfall_mm"
].first()
yesterday_rain

# %%
total_last_31_days = current_data_df["rainfall_mm"].sum()
total_last_31_days

# %%
date_beg_of_current_month = dt.date(dt.date.today().year, dt.date.today().month, 1)
total_since_beginning_of_month = current_data_df.filter(
    pl.col("date") >= date_beg_of_current_month
)["rainfall_mm"].sum()
total_since_beginning_of_month

# %% [markdown]
# So far so good, we have 2.2mm rained yesterday, 45.6mm on last slinding month and 2.2mm since beginning of month ! :)

# %%
