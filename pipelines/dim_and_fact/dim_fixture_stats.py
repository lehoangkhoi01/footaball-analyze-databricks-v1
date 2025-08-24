import dlt
from src.schemas.fields import TableNames, CommonFields, FixtureStatsFields
from src.schemas.fixture_stats_schema import FixtureStatsSchema
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import LongType
from pyspark.sql.window import Window
from src.utils.football_utils import DataFrameFootballUtils

@dlt.table(
    name=f"gold.{TableNames.DIM_FIXTURE_STATS}",
    table_properties={"quality": "gold"},
    schema=FixtureStatsSchema.get_fixture_stats_dim_schema()
)
def dim_fixtures():
    silver_fixture_stats_df = spark.read.table(f"silver.{TableNames.SILVER_FIXTURE_STATS}")
    silver_fixtures_df = spark.read.table(f"silver.{TableNames.SILVER_FIXTURES}")

    # Add surrogate key
    window_spec = Window.orderBy(CommonFields.FIXTURE_ID)
    dim_fixture_df = silver_fixture_stats_df.withColumn(
        FixtureStatsFields.FIXTURE_STAT_KEY,
        row_number().over(window_spec).cast(LongType())
    )

    # Join to get 'league_id'
    dim_fixture_stat_df = dim_fixture_df.join(
        silver_fixtures_df.select(
            CommonFields.FIXTURE_ID,
            CommonFields.LEAGUE_ID
        ),
        on=CommonFields.FIXTURE_ID,
        how='left'
    )
    dim_fixture_stat_df = dim_fixture_stat_df.drop("ingestion_time")

    return dim_fixture_stat_df