import dlt
from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, CommonFields, FixtureStatsFields, MetadataFields
from src.schemas.dim_date_schema import DimDateSchema
from src.utils.date_dim_utils import DateUtils
from datetime import datetime


spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `gold`")


@dlt.table(
    name=f"gold.{TableNames.DIM_DATES}",
    table_properties={
        "quality" : "gold"
    },
    schema=DimDateSchema.get_schema()
)
def dim_date():
    # Create date dimension
    start_date = datetime(2023, 8, 1)
    end_date = datetime(2024, 7, 31)
    dim_date_df = DateUtils.generate_date_df_with_season_sql(spark, start_date, end_date)
    return dim_date_df
