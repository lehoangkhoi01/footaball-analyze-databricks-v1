import dlt
import requests
import json
from datetime import datetime
from pyspark.sql.functions import *
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
from src.utils.data_utils import DataUtils

spark.sql("USE SCHEMA `bronze`")

@dlt.table(
    name=TableNames.BRONZE_LEAGUES,
    table_properties={
        "quality": "bronze"
    },
    comment="Transform JSON format to table columns"
)
def bronze_league():
    bronze_league_df = spark.read.table(f'staging.{TableNames.STAGING_LEAGUES}').select(
        col("record_id"),
        from_json(col("json_data"), schema=LeagueSchema.get_bronze_schema()).alias("parsed_data"),
        col("ingestion_timestamp")
    )

    # Extract the parsed_data fields to remove the prefix
    parsed_fields_df = bronze_league_df.select(
        col("record_id"),
        col("parsed_data.*"),  # This expands all fields from parsed_data without the prefix
        col("ingestion_timestamp")
    )

    # Flatten the DataFrame
    flattened_df = DataUtils.flatten_dataframe(parsed_fields_df, separator="_")

    return flattened_df



