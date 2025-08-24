import dlt
from src.schemas.fields import TableNames, CommonFields, TeamFields, DateFields, FactMatchResultFields, FactMatchResultFields
from pyspark.sql.functions import *

@dlt.table(
    name=f"gold.{TableNames.FACT_TEAM_RESULT_SUMMARY}",
    table_properties={"quality": "gold"},
    partition_cols=[DateFields.SEASON, FactMatchResultFields.COMPETITION]
)
def fact_team_result_summary():
    fact_team_result = spark.read.table(f"gold.{TableNames.FACT_TEAM_RESULT}")
    result_df = (
        fact_team_result.groupBy(FactMatchResultFields.TEAM_NAME, DateFields.SEASON, col(FactMatchResultFields.LEAGUE_NAME).alias(FactMatchResultFields.COMPETITION))
        .agg(
            count("key").alias(FactMatchResultFields.MATCH_PLAYED),
            count(when(col(FactMatchResultFields.WIN) == True, True)).alias(FactMatchResultFields.TOTAL_WIN),
            count(when(col(FactMatchResultFields.LOSE) == True, True)).alias(FactMatchResultFields.TOTAL_LOSE),
            count(when(col(FactMatchResultFields.DRAW) == True, True)).alias(FactMatchResultFields.TOTAL_DRAW),
            sum(FactMatchResultFields.GOAL_SCORED).alias(FactMatchResultFields.TOTAL_GOALS_SCORED),
            sum(FactMatchResultFields.GOAL_CONCEDED).alias(FactMatchResultFields.TOTAL_GOALS_CONCEDED),
            ((count(when(col(FactMatchResultFields.WIN) == True, True)) / count("key")) * 100).alias(FactMatchResultFields.WIN_PERCENTAGE)
        )
    )
    return result_df