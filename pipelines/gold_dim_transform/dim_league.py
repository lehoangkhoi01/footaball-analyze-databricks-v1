import dlt
from src.schemas.fields import TableNames, CommonFields, LeagueFields
from src.schemas.league_schema import LeagueSchema
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `gold`")


@dlt.table(
    name=f"gold.{TableNames.DIM_LEAGUES}",
    table_properties={
        "quality" : "gold"
    },
    schema=LeagueSchema.get_gold_schema()
)
def dim_leagues():
    silver_league_df = spark.read.table(f"silver.{TableNames.SILVER_LEAGUES}")
    dim_league_df = (silver_league_df
                 .select(
                    col(CommonFields.LEAGUE_ID).cast(LongType()),
                    col(LeagueFields.LEAGUE_NAME),
                    col(LeagueFields.TYPE),
                    col(LeagueFields.LOGO),
                    col(LeagueFields.COUNTRY),
                    col(LeagueFields.COUNTRY_FLAG),
                 ))
    # Add surrogate key
    window_spec = Window.orderBy(CommonFields.LEAGUE_ID)

    dim_league_df = dim_league_df.withColumn(
        LeagueFields.LEAGUE_KEY,
        row_number().over(window_spec).cast(LongType())
    )

    return dim_league_df