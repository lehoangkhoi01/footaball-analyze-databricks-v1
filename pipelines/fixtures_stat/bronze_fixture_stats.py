from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, MetadataFields, OtherFields
import dlt
import json
from src.schemas.fixture_stats_schema import FixtureStatsSchema
from pyspark.sql.functions import *
from src.utils.data_utils import DataUtils
import logging
import sys
from src.logging.db_logger import LoggerConfig, get_pipeline_logger

# -------------------------------------------------------------------------------
spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `bronze`")
raw_data_storage_location = spark.conf.get("raw_source_dir")
logger = get_pipeline_logger()

# -------------------------------------------------------------------------------
@dlt.table(
    name=TableNames.BRONZE_FIXTURE_STATS,
    table_properties={
        "quality": "bronze"
    },
    comment="Raw Fixture Stat data from Football API"
)
def bronze_fixture_stats():
    schema_location = spark.conf.get("bronze_schema_location")
    staging_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location}")
        .option("multiline", "true")
        .schema(FixtureStatsSchema.get_staging_schema())
        .load(f"{raw_data_storage_location}/fixture_stats")
    )

    # # Flatten the DataFrame
    flattened_df = DataUtils.flatten_dataframe(staging_df, separator="_")
    flattened_df = flattened_df.withColumn(MetadataFields.INGESTION_TIME, current_timestamp())
    return flattened_df