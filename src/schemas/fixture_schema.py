from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, DateType,
    ArrayType, LongType, TimestampType
)

from src.schemas.fields import CommonFields, FixtureFields, MetadataFields

class FixtureSchema:
    @staticmethod
    def get_bronze_schema():
        return StructType([
            StructField("fixture", StructType([
                StructField("id", IntegerType(), nullable=False),
                StructField("referee", StringType(), nullable=True),
                StructField("timezone", StringType(), nullable=True),
                StructField("date", StringType(), nullable=True),
                StructField("timestamp", LongType(), nullable=True),
                StructField("periods", StructType([
                    StructField("first", TimestampType(), nullable=True),
                    StructField("second", TimestampType(), nullable=True),
                ]), nullable=False),
                StructField("venue", StructType([
                    StructField("id", LongType(), nullable=True),
                    StructField("name", StringType(), nullable=True),
                    StructField("city", StringType(), nullable=True),
                ]), nullable=False),
                StructField("status", StructType([
                    StructField("long", StringType(), nullable=True),
                    StructField("short", StringType(), nullable=True),
                    StructField("elapsed", IntegerType(), nullable=True),
                    StructField("extra", IntegerType(), nullable=True)
                ]), nullable=False),
            ]), nullable=False),

            StructField("league", StructType([
                StructField("id", LongType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("country", StringType(), nullable=True),
                StructField("logo", StringType(), nullable=True),
                StructField("flag", StringType(), nullable=True),
                StructField("season", IntegerType(), nullable=False),
                StructField("round", StringType(), nullable=True),
                StructField("standings", BooleanType(), nullable=True),
            ]), nullable=False),

            StructField("teams", StructType([
                StructField("home", StructType([
                    StructField("id", LongType(), nullable=False),
                    StructField("name", StringType(), nullable=True),
                    StructField("logo", StringType(), nullable=True),
                    StructField("winner", BooleanType(), nullable=True)
                ]), nullable=False),

                StructField("away", StructType([
                    StructField("id", LongType(), nullable=False),
                    StructField("name", StringType(), nullable=True),
                    StructField("logo", StringType(), nullable=True),
                    StructField("winner", BooleanType(), nullable=True)
                ]), nullable=False)
            ]), nullable=False),

            StructField("goals", StructType([
                StructField("home", IntegerType(), nullable=False),
                StructField("away", IntegerType(), nullable=False)
            ]), nullable=False),

            StructField("score", StructType([
                StructField("halftime", StructType([
                    StructField("home", IntegerType(), nullable=False),
                    StructField("away", IntegerType(), nullable=False)
                ]), nullable=False),
                StructField("fulltime", StructType([
                    StructField("home", IntegerType(), nullable=False),
                    StructField("away", IntegerType(), nullable=False)
                ]), nullable=False),
                StructField("extratime", StructType([
                    StructField("home", IntegerType(), nullable=True),
                    StructField("away", IntegerType(), nullable=True)
                ]), nullable=True),
                StructField("penalty", StructType([
                    StructField("home", IntegerType(), nullable=True),
                    StructField("away", IntegerType(), nullable=True)
                ]), nullable=True)
            ]), nullable=False)
        ])
    
    @staticmethod
    def get_silver_schema():
        return StructType([
            StructField(CommonFields.FIXTURE_ID, LongType(), nullable=False),
            StructField(FixtureFields.REFEREE, StringType(), nullable=True),
            StructField(FixtureFields.TIMEZONE, StringType(), nullable=True),
            StructField(FixtureFields.DATE, DateType(), nullable=False),
            StructField(FixtureFields.TIMESTAMP, LongType(), nullable=True),
            StructField(FixtureFields.FIRST_PERIOD, TimestampType(), nullable=True),
            StructField(FixtureFields.SECOND_PERIOD, TimestampType(), nullable=True),
            StructField(FixtureFields.STADIUM_ID, LongType(), nullable=True),
            StructField(FixtureFields.STATUS, StringType(), nullable=False),
            StructField(FixtureFields.ELAPSED, IntegerType(), nullable=False),

            StructField(CommonFields.LEAGUE_ID, LongType(), nullable=False),
            StructField(FixtureFields.SEASON, IntegerType(), nullable=False),
            StructField(FixtureFields.ROUND, StringType(), nullable=False),
            StructField(FixtureFields.LEAGUE_STANDINGS, BooleanType(), nullable=True),
            StructField(FixtureFields.HOME_TEAM_ID, LongType(), nullable=False),
            StructField(FixtureFields.AWAY_TEAM_ID, LongType(), nullable=False),

            StructField(FixtureFields.GOALS_HOME, IntegerType(), nullable=False),
            StructField(FixtureFields.GOALS_AWAY, IntegerType(), nullable=False),

            StructField(FixtureFields.SCORE_HALFTIME_HOME, IntegerType(), nullable=False),
            StructField(FixtureFields.SCORE_HALFTIME_AWAY, IntegerType(), nullable=False),

            StructField(FixtureFields.SCORE_FULLTIME_HOME, IntegerType(), nullable=False),
            StructField(FixtureFields.SCORE_FULLTIME_AWAY, IntegerType(), nullable=False),

            StructField(FixtureFields.SCORE_EXTRATIME_HOME, IntegerType(), nullable=True),
            StructField(FixtureFields.SCORE_EXTRATIME_AWAY, IntegerType(), nullable=True),
            
            StructField(FixtureFields.SCORE_PENALTY_HOME, IntegerType(), nullable=True),
            StructField(FixtureFields.SCORE_PENALTY_AWAY, IntegerType(), nullable=True),
            StructField(MetadataFields.INGESTION_TIME, TimestampType(), nullable=True)
        ])
    
    @staticmethod
    def get_dim_fixture_schema():
        return StructType([
            StructField(FixtureFields.FIXTURE_KEY, LongType(), nullable=False),
            StructField(CommonFields.FIXTURE_ID, LongType(), nullable=False),
            StructField(FixtureFields.REFEREE, StringType(), nullable=True),
            StructField(FixtureFields.TIMEZONE, StringType(), nullable=True),
            StructField(FixtureFields.DATE, DateType(), nullable=False),
            StructField(FixtureFields.TIMESTAMP, LongType(), nullable=True),
            StructField(FixtureFields.FIRST_PERIOD, TimestampType(), nullable=True),
            StructField(FixtureFields.SECOND_PERIOD, TimestampType(), nullable=True),
            StructField(FixtureFields.STADIUM_ID, LongType(), nullable=True),
            StructField(FixtureFields.STATUS, StringType(), nullable=False),
            StructField(FixtureFields.ELAPSED, IntegerType(), nullable=False),

            StructField(CommonFields.LEAGUE_ID, LongType(), nullable=False),
            StructField(FixtureFields.SEASON, IntegerType(), nullable=False),
            StructField(FixtureFields.ROUND, StringType(), nullable=False),
            StructField(FixtureFields.LEAGUE_STANDINGS, BooleanType(), nullable=True),

            StructField(FixtureFields.HOME_TEAM_ID, LongType(), nullable=False),
            StructField(FixtureFields.AWAY_TEAM_ID, LongType(), nullable=False),

            StructField(FixtureFields.GOALS_HOME, IntegerType(), nullable=False),
            StructField(FixtureFields.GOALS_AWAY, IntegerType(), nullable=False),

            StructField(FixtureFields.SCORE_HALFTIME_HOME, IntegerType(), nullable=False),
            StructField(FixtureFields.SCORE_HALFTIME_AWAY, IntegerType(), nullable=False),

            StructField(FixtureFields.SCORE_FULLTIME_HOME, IntegerType(), nullable=False),
            StructField(FixtureFields.SCORE_FULLTIME_AWAY, IntegerType(), nullable=False),

            StructField(FixtureFields.SCORE_EXTRATIME_HOME, IntegerType(), nullable=True),
            StructField(FixtureFields.SCORE_EXTRATIME_AWAY, IntegerType(), nullable=True),

            StructField(FixtureFields.SCORE_PENALTY_HOME, IntegerType(), nullable=True),
            StructField(FixtureFields.SCORE_PENALTY_AWAY, IntegerType(), nullable=True),

            StructField(FixtureFields.SCORE_FULLTIME, StringType(), nullable=False),
            StructField(FixtureFields.SCORE_HALFTIME, StringType(), nullable=False),
            StructField(FixtureFields.SCORE_EXTRATIME, StringType(), nullable=True),

            StructField("__START_AT", TimestampType(), nullable=False),
            StructField("__END_AT", TimestampType(), nullable=True)
        ])