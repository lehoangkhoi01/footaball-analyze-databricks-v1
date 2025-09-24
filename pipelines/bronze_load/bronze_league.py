import dlt
import requests
import json
import logging
from datetime import datetime
from pyspark.sql.functions import *
from src.api.api_handler import APIError, APIRequestHandler
from src.api.endpoints import LEAGUES_ENDPOINT
from src.schemas.fields import TableNames, CommonFields, LeagueFields, MetadataFields
from src.schemas.league_schema import LeagueSchema
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, DateType, TimestampType,
    ArrayType, LongType,
)
from src.schemas.schema_validation import SchemaValidation, ValidationResult
from src.utils.data_utils import DataUtils

spark.sql("USE SCHEMA `bronze`")
raw_data_storage_location = spark.conf.get("raw_source_dir")

@dlt.table(
    name=TableNames.BRONZE_LEAGUES,
    table_properties={
        "quality": "bronze"
    },
    comment="Transform JSON format to table columns"
)
def bronze_league():
    schema_location = spark.conf.get("bronze_schema_location")

    staging_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location}")
        .option("multiline", "true")
        .schema(LeagueSchema.get_bronze_schema())
        .load(f"{raw_data_storage_location}/leagues")
    )

   # Flatten the DataFrame
    flattened_df = DataUtils.flatten_dataframe(staging_df, separator="_")
    flattened_df = flattened_df.withColumn(MetadataFields.INGESTION_TIME, current_timestamp())
    return flattened_df



