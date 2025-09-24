import dlt
from src.schemas.fields import TableNames, CommonFields, FixtureStatsFields, FixtureFields
from src.schemas.fixture_stats_schema import FixtureStatsSchema
from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.types import LongType
from pyspark.sql.window import Window
from src.utils.football_utils import DataFrameFootballUtils

@dlt.table(
    name=f"silver_intermediate.{TableNames.INTERMEDIATE_SILVER_FIXTURE_STATS}",
    table_properties={"quality": "silver"},
    schema=FixtureStatsSchema.get_fixture_stats_dim_schema()
)
def dim_fixtures():
    silver_fixture_stats_df = spark.read.table(f"silver.{TableNames.SILVER_FIXTURE_STATS}")
    silver_fixtures_df = spark.read.table(f"silver.{TableNames.SILVER_FIXTURES}")

    # Add surrogate key
    window_spec = Window.orderBy(CommonFields.FIXTURE_ID)
    int_silver_fixture_stats_df = silver_fixture_stats_df.withColumn(
        FixtureStatsFields.FIXTURE_STAT_KEY,
        row_number().over(window_spec).cast(LongType())
    )

    # Join to get 'league_id'
    result_df = int_silver_fixture_stats_df.join(
        silver_fixtures_df.select(
            CommonFields.FIXTURE_ID,
            CommonFields.LEAGUE_ID,
            FixtureFields.HOME_TEAM_ID,
            FixtureFields.AWAY_TEAM_ID
        ),
        on=CommonFields.FIXTURE_ID,
        how='left'
    )
    result_df = (result_df.withColumn(
        FixtureStatsFields.IS_HOME_MATCH,
        when(col(CommonFields.TEAM_ID) == col(FixtureFields.HOME_TEAM_ID), lit(True)).otherwise(lit(False))
    ).drop(FixtureFields.HOME_TEAM_ID, FixtureFields.AWAY_TEAM_ID))
    result_df = result_df.drop("ingestion_time")

    return result_df