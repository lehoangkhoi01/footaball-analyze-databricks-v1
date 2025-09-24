from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, MetadataFields
import dlt
import json
from src.schemas.team_schema import TeamSchema
from pyspark.sql.functions import *
from src.utils.data_utils import DataUtils

# -------------------------------------------------------------------------------
spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `bronze`")

raw_data_storage_location = spark.conf.get("raw_source_dir")

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
    return flattened_df

