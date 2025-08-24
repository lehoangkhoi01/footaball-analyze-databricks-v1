import dlt
from src.schemas.fields import (
    TableNames, CommonFields, FixtureStatsFields, FixtureFields,
    DateFields, TeamFields, FactMatchStatisticFields, FactTeamMatchStatsFields
)
from src.schemas.fact_match_statistics_schema import FactMatchStatisticsSchema
from pyspark.sql.functions import col, row_number, sum as pysparkSum, avg as pysparkAvg, count, when
from pyspark.sql.window import Window
from pyspark.sql.types import LongType
from src.utils.football_utils import DataFrameFootballUtils


@dlt.table(
    name=f"gold.{TableNames.FACT_TEAM_MATCH_STATS}",
    table_properties={"quality": "gold"},
)
def fact_team_match_stat():
    dim_fixtures_df = spark.read.table(f"gold.{TableNames.DIM_FIXTURES}")
    dim_fixture_stats_df = spark.read.table(f"gold.{TableNames.DIM_FIXTURE_STATS}")
    dim_teams_df = spark.read.table(f"gold.{TableNames.DIM_TEAMS}")
    dim_leagues_df = spark.read.table(f"gold.{TableNames.DIM_LEAGUES}")
    dim_dates_df = spark.read.table(f"gold.{TableNames.DIM_DATES}")

    # Join dim tables
    fact_match_stats_df = (
        dim_fixture_stats_df.alias("dim_fixture_stats")
        .join(
            dim_teams_df.alias("dim_team"),
            col(f'dim_team.{CommonFields.TEAM_ID}') == col(f'dim_fixture_stats.{CommonFields.TEAM_ID}'),
            "left"
        )
        .join(
            dim_fixtures_df.alias("dim_fixtures"),
            col(f'dim_fixtures.{CommonFields.FIXTURE_ID}') == col(f'dim_fixture_stats.{CommonFields.FIXTURE_ID}'),
            "left"
        )
        .join(
            dim_dates_df.alias("dim_dates"),
            col(f'dim_dates.{DateFields.DATE}') == col(f'dim_fixtures.{FixtureFields.DATE}'),
            "left"
        )
    )

    # Select desired fields
    fact_match_stats_df = fact_match_stats_df.select(
        col(f'dim_fixture_stats.{FixtureStatsFields.FIXTURE_STAT_KEY}').alias(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY),
        col(f'dim_fixtures.{FixtureFields.FIXTURE_KEY}').alias(FactMatchStatisticFields.DIM_FIXTURE_KEY),
        col(f'dim_fixtures.{CommonFields.FIXTURE_ID}').alias(FactMatchStatisticFields.FIXTURE_ID),
        col(f'dim_team.{CommonFields.TEAM_ID}').alias(FactMatchStatisticFields.TEAM_ID),
        col(f'dim_team.{TeamFields.TEAM_NAME}').alias(FactMatchStatisticFields.TEAM_NAME),
        col(f'dim_dates.{DateFields.SEASON}').alias(FactMatchStatisticFields.SEASON),
        col(f'dim_fixtures.{FixtureFields.LEAGUE_STANDINGS}').alias(FactMatchStatisticFields.IS_LEAGUE_STANDINGS),
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

    # Add goals details columns
    fact_match_stats_df = DataFrameFootballUtils.add_goals_columns(fact_match_stats_df, dim_fixtures_df)

    # Aggregate by team
    result_df = (
        fact_match_stats_df
        .groupBy(FactMatchStatisticFields.TEAM_NAME, FactMatchStatisticFields.SEASON)
        .agg(
            count(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY).alias(FactTeamMatchStatsFields.MATCH_PLAYED),
            # Goals summary
            pysparkSum(FactMatchStatisticFields.GOALS_SCORED).alias(FactTeamMatchStatsFields.TOTAL_GOALS_SCORED),
            pysparkAvg(FactMatchStatisticFields.GOALS_SCORED).alias(FactTeamMatchStatsFields.AVG_GOALS_SCORED),
            pysparkSum(FactMatchStatisticFields.GOALS_CONCEDED).alias(FactTeamMatchStatsFields.TOTAL_GOALS_CONCEDED),
            pysparkAvg(FactMatchStatisticFields.GOALS_CONCEDED).alias(FactTeamMatchStatsFields.AVG_GOALS_CONCEDED),

            pysparkSum(FactMatchStatisticFields.GOALS_SCORED_FIRST_HALF).alias(FactTeamMatchStatsFields.TOTAL_GOALS_FIRSTHALF),
            pysparkSum(FactMatchStatisticFields.GOALS_SCORED_SECOND_HALF).alias(FactTeamMatchStatsFields.TOTAL_GOALS_SECONDHALF),

            (count(when(col(FactMatchStatisticFields.GOALS_SCORED_FIRST_HALF) > 0, True)) / count(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY)).alias(FactTeamMatchStatsFields.SCORE_FIRST_HALF_PERCENTAGE),
            (count(when(col(FactMatchStatisticFields.GOALS_SCORED_SECOND_HALF) > 0, True)) / count(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY)).alias(FactTeamMatchStatsFields.SCORE_SECOND_HALF_PERCENTAGE),
            (count(when(col(FactMatchStatisticFields.GOALS_CONCEDED_FIRST_HALF) > 0, True)) / count(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY)).alias(FactTeamMatchStatsFields.CONCED_FIRST_HALF_PERCENTAGE),
            (count(when(col(FactMatchStatisticFields.GOALS_CONCEDED_SECOND_HALF) > 0, True)) / count(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY)).alias(FactTeamMatchStatsFields.CONCED_SECOND_HALF_PERCENTAGE),
            # Stats summary
            pysparkSum(FactMatchStatisticFields.SHOTS_ON_GOAL).alias(FactTeamMatchStatsFields.TOTAL_SHOTS_ON_GOAL),
            pysparkAvg(FactMatchStatisticFields.SHOTS_ON_GOAL).alias(FactTeamMatchStatsFields.AVG_SHOTS_ON_GOAL),
            pysparkSum(FactMatchStatisticFields.SHOTS_OFF_GOAL).alias(FactTeamMatchStatsFields.TOTAL_SHOTS_OFF_GOAL),
            pysparkSum(FactMatchStatisticFields.BLOCKED_SHOTS).alias(FactTeamMatchStatsFields.TOTAL_BLOCKED_SHOTS),
            pysparkSum(FactMatchStatisticFields.SHOTS_INSIDEBOX).alias(FactTeamMatchStatsFields.TOTAL_SHOTS_INSIDEBOX),
            pysparkAvg(FactMatchStatisticFields.SHOTS_INSIDEBOX).alias(FactTeamMatchStatsFields.AVG_SHOTS_INSIDEBOX),
            pysparkSum(FactMatchStatisticFields.SHOTS_OUTSIDEBOX).alias(FactTeamMatchStatsFields.TOTAL_SHOTS_OUTSIDEBOX),
            pysparkAvg(FactMatchStatisticFields.SHOTS_OUTSIDEBOX).alias(FactTeamMatchStatsFields.AVG_SHOTS_OUTSIDEBOX),
            pysparkSum(FactMatchStatisticFields.GOALKEEPER_SAVES).alias(FactTeamMatchStatsFields.TOTAL_GOALKEEPER_SAVES),
            pysparkSum(FactMatchStatisticFields.FOULS).alias(FactTeamMatchStatsFields.TOTAL_FOULS),
            pysparkAvg(FactMatchStatisticFields.FOULS).alias(FactTeamMatchStatsFields.AVG_FOULS),
            pysparkSum(FactMatchStatisticFields.CORNER_KICKS).alias(FactTeamMatchStatsFields.TOTAL_CORNER_KICKS),
            pysparkAvg(FactMatchStatisticFields.CORNER_KICKS).alias(FactTeamMatchStatsFields.AVG_CORNER_KICKS),
            pysparkSum(FactMatchStatisticFields.OFFSIDES).alias(FactTeamMatchStatsFields.TOTAL_OFFSIDES),
            pysparkSum(FactMatchStatisticFields.YELLOW_CARDS).alias(FactTeamMatchStatsFields.TOTAL_YELLOW_CARDS),
            pysparkAvg(FactMatchStatisticFields.YELLOW_CARDS).alias(FactTeamMatchStatsFields.AVG_YELLOW_CARDS),
            pysparkSum(FactMatchStatisticFields.RED_CARDS).alias(FactTeamMatchStatsFields.TOTAL_RED_CARDS),
            pysparkAvg(FactMatchStatisticFields.RED_CARDS).alias(FactTeamMatchStatsFields.AVG_RED_CARDS),
            pysparkAvg(FactMatchStatisticFields.BALL_POSSESSION).alias(FactTeamMatchStatsFields.AVG_BALL_POSSSESSION),
            pysparkSum(FactMatchStatisticFields.TOTAL_PASSES).alias(FactTeamMatchStatsFields.TOTAL_PASSES),
            pysparkSum(FactMatchStatisticFields.ACCURATE_PASSES).alias(FactTeamMatchStatsFields.TOTAL_ACCURATE_PASSES),
            pysparkAvg(FactMatchStatisticFields.PASSES_SUCCESS_PERCENTAGE).alias(FactTeamMatchStatsFields.AVG_PASS_SUCCESS_PERCENTAGE),
            pysparkAvg(FactMatchStatisticFields.EXPECTED_GOALS_RATE).alias(FactTeamMatchStatsFields.AVG_EXPECTED_GOALS_RATE)
        )
    )

    # Add surrogate key
    window_spec = Window.orderBy(FactMatchStatisticFields.TEAM_NAME)
    result_df = result_df.withColumn(
        FactTeamMatchStatsFields.FACT_KEY,
        row_number().over(window_spec).cast(LongType())
    )

    return result_df