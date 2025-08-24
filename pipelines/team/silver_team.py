from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, CommonFields, TeamFields, MetadataFields
import dlt
import json
from src.schemas.team_schema import TeamSchema
from src.schemas.schema_validation import SchemaValidation, ValidationResult
from pyspark.sql.functions import *
from src.utils.data_utils import DataUtils
from src.logging.db_logger import DatabricksLogger
import logging
import sys
from pyspark.sql.types import IntegerType, LongType, TimestampType

# -------------------------------------------------------------------------------
spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `silver`")

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
    name=TableNames.FLATTENED_SILVER_TEAMS,
    table_properties={
        "quality": "silver"
    },
    comment="Select, transform and enrich the bronze data for the 'teams' table",
    schema=TeamSchema.get_silver_schema()
)
@dlt.expect_or_drop("valid_team_id", "team_id IS NOT NULL")  # Add data quality expectations
def silver_team():
    # Get bronze data
    bronze_teams_df = spark.readStream.table(f'bronze.{TableNames.BRONZE_TEAMS}')
    if bronze_teams_df is None:
        raise Exception("No data found in bronze table")
    else:
        silver_team_df = (bronze_teams_df
                        .select(
                            col("team_id").alias(CommonFields.TEAM_ID).cast(LongType()),
                            col("team_name").alias(TeamFields.TEAM_NAME),
                            col("team_code").alias(TeamFields.CODE),
                            col("team_country").alias(TeamFields.COUNTRY),
                            col("team_founded").alias(TeamFields.FOUNDED).cast(IntegerType()),
                            col("team_national").alias(TeamFields.NATIONAL),
                            col("team_logo").alias(TeamFields.LOGO),
                            col("venue_id").alias(TeamFields.STADIUM_ID).cast(LongType()),
                            col("venue_name").alias(TeamFields.STADIUM_NAME),
                            col("venue_city").alias(TeamFields.CITY),
                            col("venue_address").alias(TeamFields.STADIUM_ADDRESS),
                            col("venue_capacity").alias(TeamFields.CAPACITY).cast(LongType()),
                            col("venue_surface").alias(TeamFields.STADIUM_SURFACE),
                            col("venue_image").alias(TeamFields.STADIUM_IMAGE),
                            col(MetadataFields.INGESTION_TIME).cast(TimestampType())
                            ))
        return silver_team_df



dlt.create_streaming_table(
    name=TableNames.SILVER_TEAMS,
    comment="Apply CDC to silver team table"
)

# Then you could apply changes to this manually processed table
dlt.apply_changes(
    target=TableNames.SILVER_TEAMS,
    source=TableNames.FLATTENED_SILVER_TEAMS,
    keys=[CommonFields.TEAM_ID],
    sequence_by=col(MetadataFields.INGESTION_TIME),
    stored_as_scd_type=2
)



