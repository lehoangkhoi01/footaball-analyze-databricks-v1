from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, MetadataFields
import dlt
import json
from src.schemas.team_schema import TeamSchema
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
    name=TableNames.BRONZE_TEAMS,
    table_properties={
        "quality": "bronze"
    },
    comment="Raw Teams data from Football API"
)
def bronze_team():
    schema_location = spark.conf.get("bronze_schema_location")

    staging_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location}")
        .schema(TeamSchema.get_bronze_schema())
        .load(f"{raw_data_storage_location}/teams")
    )

    # Flatten the DataFrame
    flattened_df = DataUtils.flatten_dataframe(staging_df, separator="_")
    flattened_df = flattened_df.withColumn(MetadataFields.INGESTION_TIME, current_timestamp())
    logger.info(f"Flattened DataFrame: {flattened_df.schema}")
    return flattened_df

