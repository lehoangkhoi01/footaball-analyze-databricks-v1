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
    schema=FactMatchResultSchema.get_fact_schema()
)
def fact_match_result():
    int_silver_fixtures_df = spark.read.table(f"silver_intermediate.{TableNames.INTERMEDIATE_SILVER_FIXTURES}")
    dim_teams_df = spark.read.table(f"gold.{TableNames.DIM_TEAMS}")
    dim_leagues_df = spark.read.table(f"gold.{TableNames.DIM_LEAGUES}")
    dim_dates_df = spark.read.table(f"gold.{TableNames.DIM_DATES}")

    # Join dimensions
    fact_match_result = (
        int_silver_fixtures_df.alias("int_silver_fixtures")
        .join(
            dim_teams_df.alias("dim_teams_home"),
            col(f'dim_teams_home.{CommonFields.TEAM_ID}') == col(f'int_silver_fixtures.{FixtureFields.HOME_TEAM_ID}')
        )
        .join(
            dim_teams_df.alias("dim_teams_away"),
            col(f'dim_teams_away.{CommonFields.TEAM_ID}') == col(f'int_silver_fixtures.{FixtureFields.AWAY_TEAM_ID}')
        )
        .join(
            dim_leagues_df.alias("dim_leagues"),
            col(f'dim_leagues.{CommonFields.LEAGUE_ID}') == col(f'int_silver_fixtures.{CommonFields.LEAGUE_ID}')
        )
        .join(
            dim_dates_df.alias("dim_dates"),
            col(f'dim_dates.{DateFields.DATE}') == col(f'int_silver_fixtures.{FixtureFields.DATE}')
        )
    )

    # Select desired fields
    fact_match_result = fact_match_result.select(
        col(f'int_silver_fixtures.{FixtureFields.FIXTURE_KEY}').alias(FactMatchResultFields.DIM_FIXTURE_KEY),
        col(f'dim_leagues.{LeagueFields.LEAGUE_KEY}').alias(FactMatchResultFields.DIM_LEAGUE_KEY),
        col(f'dim_teams_home.{TeamFields.TEAM_KEY}').alias(FactMatchResultFields.DIM_HOME_TEAM_KEY),
        col(f'dim_teams_away.{TeamFields.TEAM_KEY}').alias(FactMatchResultFields.DIM_AWAY_TEAM_KEY),
        col(f'dim_dates.{DateFields.DATE_KEY}').alias(FactMatchResultFields.DIM_DATE_KEY),

        col(f'dim_leagues.{CommonFields.LEAGUE_ID}').alias(FactMatchResultFields.LEAGUE_ID),
        col(f'dim_teams_home.{CommonFields.TEAM_ID}').alias(FactMatchResultFields.HOME_TEAM_ID),
        col(f'dim_teams_away.{CommonFields.TEAM_ID}').alias(FactMatchResultFields.AWAY_TEAM_ID),
        col(f'int_silver_fixtures.{CommonFields.FIXTURE_ID}').alias(FactMatchResultFields.FIXTURE_ID),
        col(f'int_silver_fixtures.{FixtureFields.GOALS_HOME}').alias(FactMatchResultFields.GOALS_HOME),
        col(f'int_silver_fixtures.{FixtureFields.GOALS_AWAY}').alias(FactMatchResultFields.GOALS_AWAY)
    )

    # Add surrogate key
    window_spec = Window.orderBy(FactMatchResultFields.DIM_FIXTURE_KEY)
    fact_match_result = fact_match_result.withColumn(
        FactMatchResultFields.MATCH_KEY,
        row_number().over(window_spec).cast(LongType())
    )
    return fact_match_result