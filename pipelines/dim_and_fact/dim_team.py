import dlt
from src.schemas.fields import TableNames, CommonFields, LeagueFields, TeamFields
from src.schemas.team_schema import TeamSchema
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import LongType
from pyspark.sql.window import Window
from src.utils.football_utils import DataFrameFootballUtils

spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `gold`")


@dlt.table(
    name=f"gold.{TableNames.DIM_TEAMS}",
    table_properties={
        "quality" : "gold"
    },
    schema=TeamSchema.get_gold_schema()
)
def dim_teams():
    silver_team_df = spark.read.table(f"silver.{TableNames.SILVER_TEAMS}")
    dim_team_df = (silver_team_df
               .select(
                   col(CommonFields.TEAM_ID).cast(LongType()),
                   col(TeamFields.TEAM_NAME),
                   col(TeamFields.CODE),
                   col(TeamFields.LOGO),
                   col(TeamFields.COUNTRY),
                   col(TeamFields.NATIONAL),
                   col(TeamFields.STADIUM_ID),
                   col(TeamFields.STADIUM_NAME),
                   col(TeamFields.STADIUM_ADDRESS),
                   col(TeamFields.CAPACITY),
                   col(TeamFields.CITY),
                   col(TeamFields.FOUNDED),
                   col(TeamFields.STADIUM_IMAGE),
                   col(TeamFields.STADIUM_SURFACE)
               ))
    # Add surrogate key
    window_spec = Window.orderBy(CommonFields.TEAM_ID)
    dim_team_df = (dim_team_df
        .withColumn(TeamFields.TEAM_KEY, row_number().over(window_spec).cast(LongType()))
        .withColumn(TeamFields.TEAM_AGE,
            year(current_date()) - col(TeamFields.FOUNDED).cast("int"))
        .withColumn(TeamFields.CAPACITY_CATEGORY, DataFrameFootballUtils.get_stadium_category(col(TeamFields.CAPACITY))))

    return dim_team_df