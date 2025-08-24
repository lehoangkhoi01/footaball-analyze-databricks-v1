from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, CommonFields, TeamFields, MetadataFields, OtherFields
import dlt
import json
from src.schemas.stats_tracking_schema import StatsTrackingSchema
from pyspark.sql.functions import *
import sys
from pyspark.sql.types import IntegerType, LongType, TimestampType
from src.api.api_handler import APIError, APIRequestHandler
from src.api.endpoints import FIXTURE_STATS_ENDPOINT
import concurrent.futures
from itertools import chain
from src.utils.fixture_stats_tracking_util import FixtureStatsHandler
from datetime import datetime
from src.logging.db_logger import LoggerConfig, get_pipeline_logger

# -------------------------------------------------------------------------------
spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `staging`")

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

logger = get_pipeline_logger()

# -------------------------------------------------------------------------------

def fetch_fixture_stats(fixture_id: int):
    with APIRequestHandler(base_url=rootUrl) as api:
        try:
            response = api.fetch_with_rate_limit(DELAY, headers, FIXTURE_STATS_ENDPOINT, params={'fixture': fixture_id})
            resultCount = response.__getitem__('results')
            errors = response.__getitem__('errors')
            if errors:
                raise ValueError(f"Errors - fetch_fixture_stats: {errors}")
            if not resultCount:
                print('Empty data from fetching Fixture Stats endpoint')
                raise ValueError(f"No stats found for fixture ID: {fixture_id}")
            else:
                fixtures_stats_data = response.get('response', [])
                return {
                            'fixture_id': fixture_id,
                            'stats': fixtures_stats_data,
                            'fetched_at': datetime.now().isoformat()
                        }
        except APIError as e:
            raise ValueError("Error from fetch_fixture_stats: ", e)
        except Exception as e:
            raise ValueError("Error from fetch_fixture_stats: ", e)


def process_combination_fetch_fixtures_stats(fixture_ids, league_id):
    results = []

    # Process with controlled parallelism
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Map fixture_ids to fetch_fixture_stats function
        future_to_fixture = {
            executor.submit(fetch_fixture_stats, fixture_id): fixture_id
            for fixture_id in fixture_ids
        }

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_fixture):
            fixture_id = future_to_fixture[future]
            try:
                result= future.result()
                result["league_id"] = league_id
                results.append(result)
            except Exception as e:
                raise Exception(f"Processing failed for fixture ID {fixture_id}: {str(e)}")
    return results

@dlt.table(
    name="tracking_fixture_stat_fectching",
    table_properties={
        "quality": "bronze"
    }
)
def tracking_fixture_stat():
    schema_location = spark.conf.get("bronze_schema_location")
    # -----------------------
    return_df = []
    fixtures_df = spark.read.table(f"bronze.{TableNames.BRONZE_FIXTURES}")
    #fixture_tracking_df = spark.readStream.table(f"bronze_intermediate.{TableNames.TRACKING_FETCH_FIXTURE_STAT}")
    league_progress_tracking_df = (spark.read
        .format("json")
        .option("multiline", "true")
        .schema(StatsTrackingSchema.get_league_stat_tracking_schema())
        .load(f"{raw_data_storage_location}/tracking/fixture_stat_league")
    )

    fixture_tracking_df = (spark.read
        .format("json")
        .option("multiline", "true")
        .schema(StatsTrackingSchema.get_fixture_stat_tracking_schema())
        .load(f"{raw_data_storage_location}/tracking/fixture_stats")
    )

    handler = FixtureStatsHandler(
        fixture_stats_tracking_df = fixture_tracking_df,
        league_progress_tracking_df = league_progress_tracking_df,
        spark = spark
    )

    fixtures_to_fetch  = handler.get_next_fixtures_to_fetch(limit=3)
    fixture_ids_list = [fixtures['fixture_id'] for fixtures in fixtures_to_fetch]
    if (len(fixtures_to_fetch) <= 0):
        raise Exception("Can not find next fixture to fetch")
    league_id = fixtures_to_fetch[0]['league_id']
    # -------------------------

    all_data = process_combination_fetch_fixtures_stats(fixture_ids_list, league_id)
    if not all_data:
        raise Exception('Error - Empty data from fetching Fixture Stats endpoint')
    else:
        # Update tracking progress of fetching Fixture stats
        fetched_fixture_id_list = [fixture['fixture_id'] for fixture in all_data]
        if fetched_fixture_id_list is None or len(fetched_fixture_id_list) <= 0:
            raise ValueError("Fetched fixture IDs list is empty.")

        # Update table tracking
        handler.mark_fixtures_fetched(fetched_fixture_id_list, league_id)
        latest_stat_tracking_df = handler.get_fixture_stats()
        logger.info(f"Stat tracking DF: {latest_stat_tracking_df.schema}")
        latest_league_progress_df = handler.get_league_progress_tracking()
        logger.info(f"League tracking DF: {latest_league_progress_df.schema}")

        # Save 2 latest tracking DF to JSON
        for row in latest_stat_tracking_df.select("league_id").distinct().collect():
            league_id = row["league_id"]
            group_df = latest_stat_tracking_df.filter(
                (col("league_id") == league_id)
            )
            dict_list = [row.asDict() for row in group_df.collect()]
            json_data = json.dumps(dict_list, indent=2, sort_keys=True, default=str)
            output_path = f"{raw_data_storage_location}/tracking/fixture_stats/league_{league_id}.json"
            dbutils.fs.put(output_path, json_data, True)

        league_progress_list = [row.asDict() for row in latest_league_progress_df.collect()]
        json_league_progress = json.dumps(league_progress_list, indent=2, sort_keys=True, default=str)
        league_tracking_file_name = f"{raw_data_storage_location}/tracking/fixture_stat_league/league_progress.json"
        dbutils.fs.put(league_tracking_file_name, json_league_progress, True)


        # Save as JSON into Volume
        json_data = json.dumps(all_data, indent=2, sort_keys=True, default=str)
        batch_file_name = f"{raw_data_storage_location}/fixture_stats/stats_{league_id}_{datetime.now()}.json"
        batch_row_count = len(all_data)
        batch_ingestion_time = str(datetime.now())
        dbutils.fs.put(batch_file_name, json_data, True)

        for record in all_data:
            return_df.append({
                "fixture_id": record["fixture_id"],
                "league_id": record["league_id"],
                "batch_file_name": batch_file_name,
                "ingestion_timestamp": batch_ingestion_time,
                "batch_row_count": batch_row_count,
        })

    return spark.createDataFrame(return_df)

