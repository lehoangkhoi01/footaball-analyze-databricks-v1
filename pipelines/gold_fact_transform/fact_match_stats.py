import dlt
from src.schemas.fields import (
    TableNames, CommonFields, FixtureStatsFields, FixtureFields,
    DateFields, TeamFields, LeagueFields, FactMatchStatisticFields
)
from src.schemas.fact_match_statistics_schema import FactMatchStatisticsSchema
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import LongType
from src.utils.football_utils import DataFrameFootballUtils


@dlt.table(
    name=f"gold.{TableNames.FACT_MATCH_STATS}",
    table_properties={"quality": "gold"},
    schema=FactMatchStatisticsSchema.get_fact_schema()
)
def fact_match_stats():
    int_silver_fixtures_df = spark.read.table(f"silver_intermediate.{TableNames.INTERMEDIATE_SILVER_FIXTURES}")
    int_silver_fixture_stats_df = spark.read.table(f"silver_intermediate.{TableNames.INTERMEDIATE_SILVER_FIXTURE_STATS}")

    dim_teams_df = spark.read.table(f"gold.{TableNames.DIM_TEAMS}")
    dim_leagues_df = spark.read.table(f"gold.{TableNames.DIM_LEAGUES}")
    dim_dates_df = spark.read.table(f"gold.{TableNames.DIM_DATES}")

    # Join dim tables
    fact_match_stats_df = (int_silver_fixture_stats_df.alias("int_silver_fixture_stats")
        .join(dim_teams_df.alias("dim_team"),
              col(f'dim_team.{CommonFields.TEAM_ID}') == col(f'int_silver_fixture_stats.{CommonFields.TEAM_ID}'),
              "left")
        .join(int_silver_fixtures_df.alias("int_silver_fixtures"),
              col(f'int_silver_fixtures.{CommonFields.FIXTURE_ID}') == col(f'int_silver_fixture_stats.{CommonFields.FIXTURE_ID}'),
              "left")
        .join(dim_leagues_df.alias("dim_leagues"),
              col(f'dim_leagues.{CommonFields.LEAGUE_ID}') == col(f'int_silver_fixture_stats.{CommonFields.LEAGUE_ID}'),
              "left")  
        .join(dim_dates_df.alias("dim_dates"),
              col(f'dim_dates.{DateFields.DATE}') == col(f'int_silver_fixtures.{FixtureFields.DATE}'),
              "left"))

    # Select desired fields
    fact_match_stats_df = fact_match_stats_df.select(
        col(f'int_silver_fixture_stats.{FixtureStatsFields.FIXTURE_STAT_KEY}').alias(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY),
        col(f'dim_leagues.{LeagueFields.LEAGUE_KEY}').alias(FactMatchStatisticFields.DIM_LEAGUE_KEY),
        col(f'dim_team.{TeamFields.TEAM_KEY}').alias(FactMatchStatisticFields.DIM_TEAM_KEY),
        col(f'int_silver_fixtures.{FixtureFields.FIXTURE_KEY}').alias(FactMatchStatisticFields.DIM_FIXTURE_KEY),
        col(f'dim_dates.{DateFields.DATE_KEY}').cast(LongType()).alias(FactMatchStatisticFields.DIM_DATE_KEY),

        col(f'int_silver_fixtures.{CommonFields.FIXTURE_ID}').alias(FactMatchStatisticFields.FIXTURE_ID),
        col(f'dim_team.{CommonFields.TEAM_ID}').alias(FactMatchStatisticFields.TEAM_ID),
    )

    # Add surrogate key
    window_spec = Window.orderBy(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY)
    fact_match_stats_df = fact_match_stats_df.withColumn(
        FactMatchStatisticFields.MATCH_STATISTIC_KEY,
        row_number().over(window_spec).cast(LongType())
    )
    return fact_match_stats_df