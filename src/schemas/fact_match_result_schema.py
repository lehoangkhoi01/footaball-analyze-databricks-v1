from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType, BooleanType

from src.schemas.fields import FactMatchResultFields

class FactMatchResultSchema:
    @staticmethod
    def get_schema():
        """
        Returns the schema for the Fact Match Result table.
        
        Returns:
            StructType: Schema definition for the Fact Match Result table.
        """
        return StructType([
            StructField(FactMatchResultFields.MATCH_KEY, LongType(), False),
            StructField(FactMatchResultFields.DIM_FIXTURE_KEY, LongType(), False),
            StructField(FactMatchResultFields.DIM_DATE_KEY, IntegerType(), False),
            StructField(FactMatchResultFields.DIM_HOME_TEAM_KEY, LongType(), False),
            StructField(FactMatchResultFields.DIM_AWAY_TEAM_KEY, LongType(), False),
            StructField(FactMatchResultFields.DIM_LEAGUE_KEY, LongType(), False),

            StructField(FactMatchResultFields.LEAGUE_NAME, StringType(), False),
            StructField(FactMatchResultFields.HOME_TEAM_NAME, StringType(), False),
            StructField(FactMatchResultFields.AWAY_TEAM_NAME, StringType(), False),
            StructField(FactMatchResultFields.SEASON, StringType(), False),
            StructField(FactMatchResultFields.ROUND, StringType(), False),
            StructField(FactMatchResultFields.MATCH_STATUS, StringType(), False),
            StructField(FactMatchResultFields.FIRST_PERIOD, TimestampType(), False),
            StructField(FactMatchResultFields.SECOND_PERIOD, TimestampType(), False),
            StructField(FactMatchResultFields.ELAPSED, IntegerType(), False),

            StructField(FactMatchResultFields.GOALS_HOME, IntegerType(), False),
            StructField(FactMatchResultFields.GOALS_AWAY, IntegerType(), False),
            StructField(FactMatchResultFields.SCORE_HALFTIME_HOME, IntegerType(), False),
            StructField(FactMatchResultFields.SCORE_HALFTIME_AWAY, IntegerType(), False),
            StructField(FactMatchResultFields.SCORE_FULLTIME_HOME, IntegerType(), False),
            StructField(FactMatchResultFields.SCORE_FULLTIME_AWAY, IntegerType(), False),
            StructField(FactMatchResultFields.SCORE_EXTRATIME_HOME, IntegerType(), False),
            StructField(FactMatchResultFields.SCORE_EXTRATIME_AWAY, IntegerType(), False),
            StructField(FactMatchResultFields.SCORE_PENALTY_HOME, IntegerType(), False),
            StructField(FactMatchResultFields.SCORE_PENALTY_AWAY, IntegerType(), False),
            
            StructField(FactMatchResultFields.TOTAL_GOALS, IntegerType(), False),
            StructField(FactMatchResultFields.GOAL_DIFF, IntegerType(), False),
            StructField(FactMatchResultFields.MATCH_RESULT, StringType(), False),
            StructField(FactMatchResultFields.IS_DRAW, BooleanType(), False),
            StructField(FactMatchResultFields.IS_HOME_WIN, BooleanType(), False),
            StructField(FactMatchResultFields.IS_AWAY_WIN, BooleanType(), False),
            StructField(FactMatchResultFields.IS_LEAGUE_STANDINGS, BooleanType(), False)
        ])