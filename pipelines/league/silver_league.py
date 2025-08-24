import dlt
import requests
import json
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from src.api.api_handler import APIError, APIRequestHandler
from src.api.endpoints import LEAGUES_ENDPOINT
from src.schemas.fields import TableNames, CommonFields, LeagueFields
from src.schemas.league_schema import LeagueSchema
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, DateType, TimestampType,
    ArrayType, LongType,
)
from src.schemas.schema_validation import SchemaValidation, ValidationResult


spark.sql("USE SCHEMA `silver`")

@dlt.table(
    name=TableNames.SILVER_LEAGUES,
    table_properties={
        "quality": "silver"
    },
    comment="Select, transform and enrich the bronze data for the 'leagues' table",
    schema=LeagueSchema.get_silver_schema()
)
def silver_leagues():
    # Get bronze data
    bronze_league_df = spark.read.table(f'bronze.{TableNames.BRONZE_LEAGUES}')

    # Select desire fields
    silver_league_df = (bronze_league_df
                            .select(
                                col("league_id").alias(CommonFields.LEAGUE_ID).cast(LongType()),
                                col("league_name").alias(LeagueFields.LEAGUE_NAME).cast(StringType()),
                                col("league_type").alias(LeagueFields.TYPE).cast(StringType()),
                                col("league_logo").alias(LeagueFields.LOGO).cast(StringType()),
                                col("country_name").alias(LeagueFields.COUNTRY).cast(StringType()),
                                col("country_flag").alias(LeagueFields.COUNTRY_FLAG).cast(StringType()),
                            ))

    # Add validation columns
    # Validate schema and data quality check
    validation_results: ValidationResult = SchemaValidation.validate_schema_and_data_quality(
        silver_league_df,
        LeagueSchema.get_silver_schema(),
        TableNames.SILVER_LEAGUES)

    # Use assert with ValidationResult
    assert validation_results.is_valid, f"Schema validation failed: {validation_results.to_str()}"

    return (silver_league_df)