from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, ArrayType, LongType, FloatType, BooleanType, DateType
)

from src.schemas.fields import CommonFields, FixtureStatsFields, MetadataFields

class FixtureStatsSchema:
    @staticmethod
    def get_staging_schema():
        return StructType([
            StructField("fixture_id", LongType(), nullable=False),
            StructField("stats", ArrayType(StructType([
                StructField("statistics", ArrayType(StructType([
                    StructField("type", StringType(), nullable=False),
                    StructField("value", StringType(), nullable=True),
                ]))),

                StructField("team", StructType([
                    StructField("id", LongType(), nullable=False),
                    StructField("name", StringType(), nullable=True),
                    StructField("logo", StringType(), nullable=True),
                ]), nullable=False),
            ]))),
            StructField("fetched_at", TimestampType(), nullable=False),
            StructField("league_id", LongType(), nullable=False)
        ])


    @staticmethod
    def get_bronze_schema():
        return StructType([
            StructField("statistics", ArrayType(StructType([
                StructField("type", StringType(), nullable=False),
                StructField("value", StringType(), nullable=True),
            ]), nullable=False)),

            StructField("team", StructType([
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("logo", StringType(), nullable=True),
            ]), nullable=False),
        ])
    
    @staticmethod
    def get_silver_schema():
        return StructType([
            StructField(CommonFields.FIXTURE_ID, LongType(), nullable=False),
            StructField(CommonFields.TEAM_ID, LongType(), nullable=False),
            StructField(FixtureStatsFields.SHOTS_ON_GOAL, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.SHOTS_OFF_GOAL, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.BLOCKED_SHOTS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.SHOTS_INSIDEBOX, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.SHOTS_OUTSIDEBOX, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.FOULS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.OFFSIDES, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.CORNER_KICKS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.BALL_POSSESSION, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.YELLOW_CARDS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.RED_CARDS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.GOALKEEPER_SAVES, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.TOTAL_PASSES, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.ACCURATE_PASSES, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.PASSES_SUCCESS_PERCENTAGE, FloatType(), nullable=True),
            StructField(FixtureStatsFields.EXPECTED_GOALS_RATE, FloatType(), nullable=True),
            StructField(MetadataFields.INGESTION_TIME, TimestampType(), nullable=False),
        ])
    
    @staticmethod
    def get_fixture_stats_dim_schema():
        return StructType([
            StructField(FixtureStatsFields.FIXTURE_STAT_KEY, LongType(), nullable=False),
            StructField(CommonFields.FIXTURE_ID, LongType(), nullable=False),
            StructField(CommonFields.TEAM_ID, LongType(), nullable=False),
            StructField(CommonFields.LEAGUE_ID, LongType(), nullable=False),
            StructField(FixtureStatsFields.SHOTS_ON_GOAL, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.SHOTS_OFF_GOAL, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.BLOCKED_SHOTS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.SHOTS_INSIDEBOX, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.SHOTS_OUTSIDEBOX, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.FOULS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.OFFSIDES, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.CORNER_KICKS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.BALL_POSSESSION, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.YELLOW_CARDS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.RED_CARDS, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.GOALKEEPER_SAVES, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.TOTAL_PASSES, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.ACCURATE_PASSES, IntegerType(), nullable=True),
            StructField(FixtureStatsFields.PASSES_SUCCESS_PERCENTAGE, FloatType(), nullable=True),
            StructField(FixtureStatsFields.EXPECTED_GOALS_RATE, FloatType(), nullable=True),
            StructField("__START_AT", TimestampType(), nullable=False),
            StructField("__END_AT", TimestampType(), nullable=True)
        ])