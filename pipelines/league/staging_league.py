import dlt
import requests
import json
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from src.api.api_handler import APIError, APIRequestHandler
from src.api.endpoints import LEAGUES_ENDPOINT
from src.schemas.fields import TableNames
from src.schemas.league_schema import LeagueSchema


spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `staging`")

# Step 1: Raw API data (creates a table/view)
@dlt.table(
    name=TableNames.STAGING_LEAGUES,
    table_properties={
        "quality": "bronze"
    },
    comment="Raw Leagues data from Football API"
)
def raw_api_data():
    rootUrl = dbutils.secrets.get(scope="football-analyze", key="api-url")
    api_key = dbutils.secrets.get(scope="football-analyze", key="api-key")
    api_host = spark.conf.get("api-host")
    raw_data_storage_location = spark.conf.get("raw_source_dir")

    # Define headers
    headers = {
        'x-rapidapi-host': api_host,
        'x-rapidapi-key': api_key
    }

    with APIRequestHandler(base_url=rootUrl) as api:
        api.set_headers(headers)
        try:
            # GET request
            response = api.get(LEAGUES_ENDPOINT)
            resultCount = response.__getitem__('results')
            data = response.__getitem__('response')
            if not resultCount:
                raise Exception("No result")
            else:
                # Save JSON file in Volume
                dbutils.fs.put(
                    f"{raw_data_storage_location}/leagues/leagues.json",
                    json.dumps(data, indent=2),
                    True
                )

                processed_data = []
                for i, item in enumerate(data):
                    processed_data.append({
                        "record_id": i,
                        "json_data": json.dumps(item),
                        "ingestion_timestamp": str(datetime.now())
                    })
                return spark.createDataFrame(processed_data)
        except APIError as e:
            print(f"API Error: {e}")
            print(f"Status Code: {e.status_code}")
            raise Exception(f"API Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise Exception(f"Unexpected error: {e}")
