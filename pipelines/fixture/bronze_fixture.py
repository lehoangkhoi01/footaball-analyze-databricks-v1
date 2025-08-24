from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, MetadataFields, OtherFields
import dlt
import json
from src.schemas.fixture_schema import FixtureSchema
from pyspark.sql.functions import *
from src.utils.data_utils import DataUtils
from src.logging.db_logger import DatabricksLogger
import logging
import sys

# -------------------------------------------------------------------------------
spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `bronze`")

raw_data_storage_location = spark.conf.get("raw_source_dir")

default_logger = DatabricksLogger().get_logger()

# Set up logging configuration
logger = logging.getLogger("DLTLogger")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout) # Change to sys.stderr for stderr
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

# Avoid duplicate handlers if rerun in notebook
if not logger.handlers:
  logger.addHandler(handler)


# -------------------------------------------------------------------------------
@dlt.table(
    name=TableNames.BRONZE_FIXTURES,
    table_properties={
        "quality": "bronze"
    },
    comment="Raw Fixtures data from Football API"
)
def bronze_fixture():
    schema_location = spark.conf.get("bronze_schema_location")
    staging_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location}")
        .option("multiline", "true")
        .schema(FixtureSchema.get_bronze_schema())
        .load(f"{raw_data_storage_location}/fixtures")
    )

    # Flatten the DataFrame
    flattened_df = DataUtils.flatten_dataframe(staging_df, separator="_")
    flattened_df = flattened_df.withColumn(MetadataFields.INGESTION_TIME, current_timestamp())
    return flattened_df


@dlt.table(
    name="write_tracking_fixture_stats",
    table_properties={
        "quality": "bronze"
    },
    comment="Raw Fixtures data from Football API"
)
def write_fixture():
    schema_location = spark.conf.get("bronze_schema_location")
    staging_df = (spark.read
        .format("json")
        .option("multiline", "true")
        .schema(FixtureSchema.get_bronze_schema())
        .load(f"{raw_data_storage_location}/fixtures")
    )

    # Flatten the DataFrame
    flattened_df = DataUtils.flatten_dataframe(staging_df, separator="_")
    flattened_df = flattened_df.withColumn(MetadataFields.INGESTION_TIME, current_timestamp())

    # Initialize tracking JSON files for fixture statistics
    ##  ---------- Fixture stat tracking -----------------
    fixture_stats_tracking_df = (flattened_df
        .select(
            col("fixture_id"),
            col("league_id")
        )
        .withColumn("is_fetched_stat", lit(False))
        .withColumn("fetch_stat_time", lit(None).cast("timestamp")))
    # Write fixture_stats_tracking_df to JSON file
    # Write each group to a separate JSON file by league_id and season
    for row in fixture_stats_tracking_df.select("league_id").distinct().collect():
        league_id = row["league_id"]
        group_df = fixture_stats_tracking_df.filter(
            (col("league_id") == league_id)
        )
        dict_list = [row.asDict() for row in group_df.collect()]
        json_data = json.dumps(dict_list, indent=2)
        output_path = f"{raw_data_storage_location}/tracking/fixture_stats/league_{league_id}.json"
        dbutils.fs.put(output_path, json_data, True)
    return flattened_df





