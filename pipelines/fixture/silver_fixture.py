from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, CommonFields, FixtureFields, MetadataFields
import dlt
import json
from src.schemas.fixture_schema import FixtureSchema
from src.schemas.schema_validation import SchemaValidation, ValidationResult
from pyspark.sql.functions import *
from src.utils.data_utils import DataUtils
from src.logging.db_logger import DatabricksLogger
import logging
import sys
from pyspark.sql.types import IntegerType, LongType, TimestampType, DateType

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
    name=f"silver_intermediate.{TableNames.FLATTENED_BRONZE_FIXTURES}",
    table_properties={
        "quality": "bronze"
    },
    comment="Select, transform and enrich the bronze data for the 'fixture' table",
    schema=FixtureSchema.get_silver_schema() # use silver layer schema for validation
)
def flatten_fixture():
    # Get bronze data
    bronze_fixtures_df = spark.readStream.table(f'bronze.{TableNames.BRONZE_FIXTURES}')
    if bronze_fixtures_df is None:
        raise Exception("No data found in bronze table")
    else:
        flatten_fixture_df = (bronze_fixtures_df
                        .select(
                            col("fixture_id").alias(CommonFields.FIXTURE_ID).cast(LongType()),
                            col("fixture_referee").alias(FixtureFields.REFEREE),
                            col("fixture_timezone").alias(FixtureFields.TIMEZONE),
                            col("fixture_date").alias(FixtureFields.DATE).cast(DateType()),
                            col("fixture_timestamp").alias(FixtureFields.TIMESTAMP).cast(LongType()),
                            col("fixture_periods_first").alias(FixtureFields.FIRST_PERIOD).cast(TimestampType()),
                            col("fixture_periods_second").alias(FixtureFields.SECOND_PERIOD).cast(TimestampType()),

                            col("fixture_venue_id").alias(FixtureFields.STADIUM_ID),
                            col("fixture_status_long").alias(FixtureFields.STATUS),
                            col("fixture_status_elapsed").alias(FixtureFields.ELAPSED).cast(IntegerType()),

                            col("league_id").alias(CommonFields.LEAGUE_ID).cast(LongType()),
                            col("league_season").alias(FixtureFields.SEASON).cast(IntegerType()),
                            col("league_round").alias(FixtureFields.ROUND),
                            col("league_standings").alias(FixtureFields.LEAGUE_STANDINGS),

                            col("teams_home_id").alias(FixtureFields.HOME_TEAM_ID).cast(LongType()),
                            col("teams_away_id").alias(FixtureFields.AWAY_TEAM_ID).cast(LongType()),

                            col("goals_home").alias(FixtureFields.GOALS_HOME).cast(IntegerType()),
                            col("goals_away").alias(FixtureFields.GOALS_AWAY).cast(IntegerType()),

                            col("score_halftime_home").alias(FixtureFields.SCORE_HALFTIME_HOME).cast(IntegerType()),
                            col("score_halftime_away").alias(FixtureFields.SCORE_HALFTIME_AWAY).cast(IntegerType()),

                            col("score_fulltime_home").alias(FixtureFields.SCORE_FULLTIME_HOME).cast(IntegerType()),
                            col("score_fulltime_away").alias(FixtureFields.SCORE_FULLTIME_AWAY).cast(IntegerType()),
                            (col("score_extratime_home").alias(FixtureFields.SCORE_EXTRATIME_HOME) if "score_extratime" in bronze_fixtures_df.columns else lit(0))
                                .alias(FixtureFields.SCORE_EXTRATIME_HOME),
                            (col("score_extratime_away").alias(FixtureFields.SCORE_EXTRATIME_AWAY) if "score_extratime" in bronze_fixtures_df.columns else lit(0))
                                .alias(FixtureFields.SCORE_EXTRATIME_AWAY),
                            (col("score_penalty_home").alias(FixtureFields.SCORE_PENALTY_HOME) if "score_penalty" in bronze_fixtures_df.columns else lit(0))
                                .alias(FixtureFields.SCORE_PENALTY_HOME),
                            (col("score_penalty_away").alias(FixtureFields.SCORE_PENALTY_AWAY) if "score_penalty" in bronze_fixtures_df.columns else lit(0)
                                .alias(FixtureFields.SCORE_PENALTY_AWAY)),
                            col(MetadataFields.INGESTION_TIME).cast(TimestampType())
                        ))
        return flatten_fixture_df


dlt.create_streaming_table(
    name=TableNames.SILVER_FIXTURES,
    comment="Silver fixture table with SCD type 2",
    table_properties={"quality" : "silver"},
    partition_cols=[CommonFields.LEAGUE_ID]
)

# Then you could apply changes to this manually processed table
dlt.apply_changes(
    target=TableNames.SILVER_FIXTURES,
    source=f"silver_intermediate.{TableNames.FLATTENED_BRONZE_FIXTURES}",
    keys=[CommonFields.FIXTURE_ID],
    sequence_by=col(MetadataFields.INGESTION_TIME),
    stored_as_scd_type=2
)



