import dlt
from src.schemas.fields import (
    TableNames, CommonFields, FixtureStatsFields, FixtureFields,
    DateFields, TeamFields, LeagueFields, FactMatchResultFields
)
from src.schemas.fact_match_result_schema import FactMatchResultSchema
from pyspark.sql.functions import col, lit, row_number
from src.utils.football_utils import DataFrameFootballUtils
from pyspark.sql.window import Window
from pyspark.sql.types import LongType



@dlt.table(
    name=f"gold.{TableNames.FACT_MATCH_RESULT}",
    table_properties={"quality": "gold"},
    schema=FactMatchResultSchema.get_schema()
)
def fact_match_result():
    dim_fixtures_df = spark.read.table(f"gold.{TableNames.DIM_FIXTURES}")
    dim_fixtures_stats_df = spark.read.table(f"gold.{TableNames.DIM_FIXTURE_STATS}")
    dim_teams_df = spark.read.table(f"gold.{TableNames.DIM_TEAMS}")
    dim_leagues_df = spark.read.table(f"gold.{TableNames.DIM_LEAGUES}")
    dim_dates_df = spark.read.table(f"gold.{TableNames.DIM_DATES}")

    # Join dimensions
    fact_match_result = (
        dim_fixtures_df.alias("dim_fixtures")
        .join(
            dim_teams_df.alias("dim_teams_home"),
            col(f'dim_teams_home.{CommonFields.TEAM_ID}') == col(f'dim_fixtures.{FixtureFields.HOME_TEAM_ID}')
        )
        .join(
            dim_teams_df.alias("dim_teams_away"),
            col(f'dim_teams_away.{CommonFields.TEAM_ID}') == col(f'dim_fixtures.{FixtureFields.AWAY_TEAM_ID}')
        )
        .join(
            dim_leagues_df.alias("dim_leagues"),
            col(f'dim_leagues.{CommonFields.LEAGUE_ID}') == col(f'dim_fixtures.{CommonFields.LEAGUE_ID}')
        )
        .join(
            dim_dates_df.alias("dim_dates"),
            col(f'dim_dates.{DateFields.DATE}') == col(f'dim_fixtures.{FixtureFields.DATE}')
        )
    )

    # Select desired fields
    fact_match_result = fact_match_result.select(
        col(f'dim_fixtures.{FixtureFields.FIXTURE_KEY}').alias(FactMatchResultFields.DIM_FIXTURE_KEY),
        col(f'dim_leagues.{LeagueFields.LEAGUE_KEY}').alias(FactMatchResultFields.DIM_LEAGUE_KEY),
        col(f'dim_teams_home.{TeamFields.TEAM_KEY}').alias(FactMatchResultFields.DIM_HOME_TEAM_KEY),
        col(f'dim_teams_away.{TeamFields.TEAM_KEY}').alias(FactMatchResultFields.DIM_AWAY_TEAM_KEY),
        col(f'dim_dates.{DateFields.DATE_KEY}').alias(FactMatchResultFields.DIM_DATE_KEY),
        col(f'dim_leagues.{LeagueFields.LEAGUE_NAME}').alias(FactMatchResultFields.LEAGUE_NAME),
        col(f'dim_teams_home.{TeamFields.TEAM_NAME}').alias(FactMatchResultFields.HOME_TEAM_NAME),
        col(f'dim_teams_away.{TeamFields.TEAM_NAME}').alias(FactMatchResultFields.AWAY_TEAM_NAME),
        col(f'dim_dates.{DateFields.SEASON}').alias(FactMatchResultFields.SEASON),
        col(f'dim_fixtures.{FixtureFields.ROUND}').alias(FactMatchResultFields.ROUND),
        col(f'dim_fixtures.{FixtureFields.STATUS}').alias(FactMatchResultFields.MATCH_STATUS),
        col(f'dim_fixtures.{FixtureFields.ELAPSED}').alias(FactMatchResultFields.ELAPSED),
        col(f'dim_fixtures.{FixtureFields.FIRST_PERIOD}').alias(FactMatchResultFields.FIRST_PERIOD),
        col(f'dim_fixtures.{FixtureFields.SECOND_PERIOD}').alias(FactMatchResultFields.SECOND_PERIOD),
        col(f'dim_fixtures.{FixtureFields.GOALS_HOME}').alias(FactMatchResultFields.GOALS_HOME),
        col(f'dim_fixtures.{FixtureFields.GOALS_AWAY}').alias(FactMatchResultFields.GOALS_AWAY),
        col(f'dim_fixtures.{FixtureFields.SCORE_HALFTIME_HOME}').alias(FactMatchResultFields.SCORE_HALFTIME_HOME),
        col(f'dim_fixtures.{FixtureFields.SCORE_HALFTIME_AWAY}').alias(FactMatchResultFields.SCORE_HALFTIME_AWAY),
        col(f'dim_fixtures.{FixtureFields.SCORE_FULLTIME_HOME}').alias(FactMatchResultFields.SCORE_FULLTIME_HOME),
        col(f'dim_fixtures.{FixtureFields.SCORE_FULLTIME_AWAY}').alias(FactMatchResultFields.SCORE_FULLTIME_AWAY),
        col(f'dim_fixtures.{FixtureFields.SCORE_EXTRATIME_HOME}').alias(FactMatchResultFields.SCORE_EXTRATIME_HOME),
        col(f'dim_fixtures.{FixtureFields.SCORE_EXTRATIME_AWAY}').alias(FactMatchResultFields.SCORE_EXTRATIME_AWAY),
        col(f'dim_fixtures.{FixtureFields.SCORE_PENALTY_HOME}').alias(FactMatchResultFields.SCORE_PENALTY_HOME),
        col(f'dim_fixtures.{FixtureFields.SCORE_PENALTY_AWAY}').alias(FactMatchResultFields.SCORE_PENALTY_AWAY),
        col(f'dim_fixtures.{FixtureFields.LEAGUE_STANDINGS}').alias(FactMatchResultFields.IS_LEAGUE_STANDINGS),
    )

    # Add surrogate key
    window_spec = Window.orderBy(FactMatchResultFields.DIM_FIXTURE_KEY)
    fact_match_result = fact_match_result.withColumn(
        FactMatchResultFields.MATCH_KEY,
        row_number().over(window_spec).cast(LongType())
    )

    # Add calculated columns
    fact_match_result = (
        fact_match_result
        .withColumn(
            FactMatchResultFields.TOTAL_GOALS,
            (col(FactMatchResultFields.GOALS_HOME).cast("int") + col(FactMatchResultFields.GOALS_AWAY).cast("int"))
        )
        .withColumn(
            FactMatchResultFields.GOAL_DIFF,
            (col(FactMatchResultFields.GOALS_HOME).cast("int") - col(FactMatchResultFields.GOALS_AWAY).cast("int"))
        )
        .withColumn(
            FactMatchResultFields.MATCH_RESULT,
            DataFrameFootballUtils.get_match_result(
                col(FactMatchResultFields.GOALS_HOME),
                col(FactMatchResultFields.GOALS_AWAY)
            )
        )
        .withColumn(FactMatchResultFields.IS_HOME_WIN, col(FactMatchResultFields.GOALS_HOME) > col(FactMatchResultFields.GOALS_AWAY))
        .withColumn(FactMatchResultFields.IS_AWAY_WIN, col(FactMatchResultFields.GOALS_HOME) < col(FactMatchResultFields.GOALS_AWAY))
        .withColumn(FactMatchResultFields.IS_DRAW, col(FactMatchResultFields.GOALS_HOME) == col(FactMatchResultFields.GOALS_AWAY))
    )
    return fact_match_result