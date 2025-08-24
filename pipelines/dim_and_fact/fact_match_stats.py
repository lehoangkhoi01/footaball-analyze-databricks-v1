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
)
def fact_match_stats():
    dim_fixtures_df = spark.read.table(f"gold.{TableNames.DIM_FIXTURES}")
    dim_fixture_stats_df = spark.read.table(f"gold.{TableNames.DIM_FIXTURE_STATS}")
    dim_teams_df = spark.read.table(f"gold.{TableNames.DIM_TEAMS}")
    dim_leagues_df = spark.read.table(f"gold.{TableNames.DIM_LEAGUES}")
    dim_dates_df = spark.read.table(f"gold.{TableNames.DIM_DATES}")

    # Join dim tables
    fact_match_stats_df = (dim_fixture_stats_df.alias("dim_fixture_stats")
        .join(dim_teams_df.alias("dim_team"),
              col(f'dim_team.{CommonFields.TEAM_ID}') == col(f'dim_fixture_stats.{CommonFields.TEAM_ID}'),
              "left")
        .join(dim_fixtures_df.alias("dim_fixtures"),
              col(f'dim_fixtures.{CommonFields.FIXTURE_ID}') == col(f'dim_fixture_stats.{CommonFields.FIXTURE_ID}'),
              "left") 
        .join(dim_dates_df.alias("dim_dates"),
              col(f'dim_dates.{DateFields.DATE}') == col(f'dim_fixtures.{FixtureFields.DATE}'),
              "left"))

    # Select desired fields
    fact_match_stats_df = fact_match_stats_df.select(
        col(f'dim_fixture_stats.{FixtureStatsFields.FIXTURE_STAT_KEY}').alias(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY),
        col(f'dim_team.{TeamFields.TEAM_KEY}').alias(FactMatchStatisticFields.DIM_TEAM_KEY),
        col(f'dim_fixtures.{FixtureFields.FIXTURE_KEY}').alias(FactMatchStatisticFields.DIM_FIXTURE_KEY),
        col(f'dim_dates.{DateFields.DATE_KEY}').cast(LongType()).alias(FactMatchStatisticFields.DIM_DATE_KEY),
        col(f'dim_fixtures.{CommonFields.FIXTURE_ID}').alias(FactMatchStatisticFields.FIXTURE_ID),
        col(f'dim_team.{CommonFields.TEAM_ID}').alias(FactMatchStatisticFields.TEAM_ID),
        col(f'dim_team.{TeamFields.TEAM_NAME}').alias(FactMatchStatisticFields.TEAM_NAME),
        col(f'dim_dates.{DateFields.SEASON}').alias(FactMatchStatisticFields.SEASON),
        col(f'dim_fixtures.{FixtureFields.ROUND}').alias(FactMatchStatisticFields.ROUND),
        col(f'dim_fixtures.{FixtureFields.LEAGUE_STANDINGS}').alias(FactMatchStatisticFields.IS_LEAGUE_STANDINGS),
        col(f'dim_fixtures.{FixtureFields.STATUS}').alias(FactMatchStatisticFields.MATCH_STATUS),
        col(f'dim_fixtures.{FixtureFields.FIRST_PERIOD}').alias(FactMatchStatisticFields.FIRST_PERIOD),
        col(f'dim_fixtures.{FixtureFields.SECOND_PERIOD}').alias(FactMatchStatisticFields.SECOND_PERIOD),
        col(f'dim_fixtures.{FixtureFields.ELAPSED}').alias(FactMatchStatisticFields.ELAPSED),
        col(f'dim_fixture_stats.{FixtureStatsFields.SHOTS_ON_GOAL}').alias(FactMatchStatisticFields.SHOTS_ON_GOAL),
        col(f'dim_fixture_stats.{FixtureStatsFields.SHOTS_OFF_GOAL}').alias(FactMatchStatisticFields.SHOTS_OFF_GOAL),
        col(f'dim_fixture_stats.{FixtureStatsFields.BLOCKED_SHOTS}').alias(FactMatchStatisticFields.BLOCKED_SHOTS),
        col(f'dim_fixture_stats.{FixtureStatsFields.SHOTS_INSIDEBOX}').alias(FactMatchStatisticFields.SHOTS_INSIDEBOX),
        col(f'dim_fixture_stats.{FixtureStatsFields.SHOTS_OUTSIDEBOX}').alias(FactMatchStatisticFields.SHOTS_OUTSIDEBOX),
        col(f'dim_fixture_stats.{FixtureStatsFields.GOALKEEPER_SAVES}').alias(FactMatchStatisticFields.GOALKEEPER_SAVES),
        col(f'dim_fixture_stats.{FixtureStatsFields.FOULS}').alias(FactMatchStatisticFields.FOULS),
        col(f'dim_fixture_stats.{FixtureStatsFields.CORNER_KICKS}').alias(FactMatchStatisticFields.CORNER_KICKS),
        col(f'dim_fixture_stats.{FixtureStatsFields.OFFSIDES}').alias(FactMatchStatisticFields.OFFSIDES),
        col(f'dim_fixture_stats.{FixtureStatsFields.YELLOW_CARDS}').alias(FactMatchStatisticFields.YELLOW_CARDS),
        col(f'dim_fixture_stats.{FixtureStatsFields.RED_CARDS}').alias(FactMatchStatisticFields.RED_CARDS),
        col(f'dim_fixture_stats.{FixtureStatsFields.BALL_POSSESSION}').alias(FactMatchStatisticFields.BALL_POSSESSION),
        col(f'dim_fixture_stats.{FixtureStatsFields.TOTAL_PASSES}').alias(FactMatchStatisticFields.TOTAL_PASSES),
        col(f'dim_fixture_stats.{FixtureStatsFields.ACCURATE_PASSES}').alias(FactMatchStatisticFields.ACCURATE_PASSES),
        col(f'dim_fixture_stats.{FixtureStatsFields.PASSES_SUCCESS_PERCENTAGE}').alias(FactMatchStatisticFields.PASSES_SUCCESS_PERCENTAGE),
        col(f'dim_fixture_stats.{FixtureStatsFields.EXPECTED_GOALS_RATE}').alias(FactMatchStatisticFields.EXPECTED_GOALS_RATE)
    )

    # Add surrogate key
    window_spec = Window.orderBy(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY)
    fact_match_stats_df = fact_match_stats_df.withColumn(
        FactMatchStatisticFields.MATCH_STATISTIC_KEY,
        row_number().over(window_spec).cast(LongType())
    )

    # Add goals details columns
    fact_match_stats_df = DataFrameFootballUtils.add_goals_columns(fact_match_stats_df, dim_fixtures_df)
    return fact_match_stats_df