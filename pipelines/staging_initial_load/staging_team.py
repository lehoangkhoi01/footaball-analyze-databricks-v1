import dlt
import requests
import json
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from src.api.api_handler import APIError, APIRequestHandler
from src.api.endpoints import TEAMS_ENDPOINT
from src.schemas.fields import TableNames
from src.schemas.league_schema import LeagueSchema
from src.utils.football_utils import FootballUtils
import concurrent.futures
from itertools import chain

# -------------------------------------------------------------------------------

spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `staging`")

# For testing purpost, we pre select which 'league' we want to fetch data from
DESIRED_LEAGUES = [140, 39, 45, 2, 3, 143, 135]
AVAILABLE_SEASON = [2022, 2023]

rootUrl = dbutils.secrets.get(scope="football-analyze", key="api-url")
api_key = dbutils.secrets.get(scope="football-analyze", key="api-key")
api_host = spark.conf.get("api-host")
raw_data_storage_location = spark.conf.get("raw_source_dir")

# Define headers
headers = {
    'x-rapidapi-host': api_host,
    'x-rapidapi-key': api_key
}

REQUESTS_PER_MINUTE = 10
DELAY = 60 / REQUESTS_PER_MINUTE  # 6 seconds between requests

# --------------------------------------------------------------------------------------

def fetch_teams(league_id, season=2023):
    with APIRequestHandler(base_url=rootUrl) as api:
        try:
            response = api.fetch_with_rate_limit(
                DELAY,
                headers,
                TEAMS_ENDPOINT,
                params={'league': league_id, 'season': season})
            teams_data = response.get('response', [])
            if teams_data:
                print(f'There are {len(teams_data)} teams for League ID: {league_id} in season {season}')
            return teams_data
        except APIError as e:
            raise Exception("API Error: ", e)
        except Exception as e:
            raise Exception("Unexpected error: ", e)

# Define function for fetching teams with combinations of league and season
def process_combination_fetch_teams():
    # Generate all league_id/year combinations
    with FootballUtils(DESIRED_LEAGUES, AVAILABLE_SEASON) as football_utils:
        combinations = football_utils.combineLeagueAndSeason()

    results = []

    # Process with controlled parallelism
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Map combinations to fetch_teams function
        future_to_combo = {
            executor.submit(fetch_teams, league_id, year): (league_id, year)
            for league_id, year in combinations
        }

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_combo):
            league_id, year = future_to_combo[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Processing failed for {league_id}/{year}: {str(e)}")
    return results


# Step 1: Raw API data (creates a table/view)
@dlt.table(
    name=TableNames.STAGING_TEAMS,
    table_properties={
        "quality": "bronze"
    },
    comment="Raw Teams data from Football API"
)
def raw_api_data():

    # Generate all league_id/year combinations
    with FootballUtils(DESIRED_LEAGUES, AVAILABLE_SEASON) as football_utils:
        combinations = football_utils.combineLeagueAndSeason()

    tracking_staging = []

    # Process with controlled parallelism
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Map combinations to fetch_teams function
        future_to_combo = {
            executor.submit(fetch_teams, league_id, year): (league_id, year)
            for league_id, year in combinations
        }

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_combo):
            league_id, year = future_to_combo[future]
            try:
                result = future.result()
                if not result:
                    raise Exception('Empty team data')
                #flattenData = list(chain.from_iterable(result))
                json_data = json.dumps(result, indent=2)
                file_name = f"{raw_data_storage_location}/teams/teams_{league_id}_{year}.json"
                # Save JSON file format
                dbutils.fs.put(file_name, json_data, True)
                tracking_staging.append({
                    "league_id": league_id,
                    "year": year,
                    "file_name": file_name,
                    "ingestion_timestamp": str(datetime.now())
                })
            except Exception as e:
                raise Exception(f"Processing failed for {league_id}/{year}: {str(e)}")

    return spark.createDataFrame(tracking_staging)

    # --------------------------------------------------------
