from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType, FloatType
from src.schemas.fields import FactMatchStatisticFields

class FactMatchStatisticsSchema:
    @staticmethod
    def get_schema():
        return StructType([
            StructField(FactMatchStatisticFields.MATCH_STATISTIC_KEY, LongType(), nullable=False),
            StructField(FactMatchStatisticFields.DIM_FIXTURE_KEY, LongType(), nullable=False),
            StructField(FactMatchStatisticFields.DIM_FIXTURE_STAT_KEY, LongType(), nullable=False),
            StructField(FactMatchStatisticFields.DIM_DATE_KEY, LongType(), nullable=False),
            StructField(FactMatchStatisticFields.DIM_TEAM_KEY, LongType(), nullable=False),
            StructField(FactMatchStatisticFields.DIM_LEAGUE_KEY, LongType(), nullable=False),

            StructField(FactMatchStatisticFields.FIXTURE_ID, LongType(), nullable=False),
            StructField(FactMatchStatisticFields.TEAM_ID, LongType(), nullable=False),   
            StructField(FactMatchStatisticFields.LEAGUE_NAME, StringType(), nullable=False),
            StructField(FactMatchStatisticFields.TEAM_NAME, StringType(), nullable=False),
            StructField(FactMatchStatisticFields.SEASON, StringType(), nullable=False),
            StructField(FactMatchStatisticFields.ROUND, StringType(), nullable=False),
            StructField(FactMatchStatisticFields.IS_LEAGUE_STANDINGS, BooleanType(), nullable=False),
            StructField(FactMatchStatisticFields.MATCH_RESULT, StringType(), nullable=False), #
            StructField(FactMatchStatisticFields.MATCH_STATUS, StringType(), nullable=False),
            StructField(FactMatchStatisticFields.FIRST_PERIOD, TimestampType(), nullable=False),
            StructField(FactMatchStatisticFields.SECOND_PERIOD, TimestampType(), nullable=False),
            StructField(FactMatchStatisticFields.ELAPSED, IntegerType(), nullable=False),

            StructField(FactMatchStatisticFields.GOALS_SCORED, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_CONCEDED, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_SCORED_FIRST_HALF, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_CONCEDED_FIRST_HALF, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_SCORED_SECOND_HALF, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_CONCEDED_SECOND_HALF, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_SCORED_EXTRATIME, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_CONCEDED_EXTRATIME, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_SCORED_PENALTY, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALS_CONCEDED_PENALTY, IntegerType(), nullable=False),

            StructField(FactMatchStatisticFields.SHOTS_ON_GOAL, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.SHOTS_OFF_GOAL, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.BLOCKED_SHOTS, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.SHOTS_INSIDEBOX, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.SHOTS_OUTSIDEBOX, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.GOALKEEPER_SAVES, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.FOULS, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.OFFSIDES, IntegerType(), nullable=True),
            StructField(FactMatchStatisticFields.CORNER_KICKS, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.BALL_POSSESSION, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.YELLOW_CARDS, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.RED_CARDS, IntegerType(), nullable=False),  
            StructField(FactMatchStatisticFields.TOTAL_PASSES, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.ACCURATE_PASSES, IntegerType(), nullable=False),
            StructField(FactMatchStatisticFields.PASSES_SUCCESS_PERCENTAGE, FloatType(), nullable=False),
            StructField(FactMatchStatisticFields.EXPECTED_GOALS_RATE, FloatType(), nullable=False),
        ])