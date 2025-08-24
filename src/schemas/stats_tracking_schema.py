from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, DateType, TimestampType,
    ArrayType, LongType, FloatType
)

from src.schemas.fields import CommonFields, TeamFields, MetadataFields, OtherFields

class StatsTrackingSchema():
    @staticmethod
    def get_league_stat_tracking_schema():
        return StructType([
            StructField(CommonFields.LEAGUE_ID, LongType(), nullable=False),
            StructField(OtherFields.YEAR, IntegerType(), nullable=False),
            StructField(OtherFields.TOTAL_FIXTURES, IntegerType(), nullable=False),
            StructField(OtherFields.FETCHED_COUNT, IntegerType(), nullable=False),
            StructField(OtherFields.FETCHED_PERCENTAGE, FloatType(), nullable=False),
            StructField(OtherFields.IS_COMPLETE, BooleanType(), nullable=False),
            StructField(OtherFields.LAST_UPDATED, TimestampType(), nullable=True)
        ])

    @staticmethod
    def get_fixture_stat_tracking_schema():
        return StructType([
            StructField(CommonFields.FIXTURE_ID, LongType(), nullable=False),
            StructField(CommonFields.LEAGUE_ID, LongType(), nullable=False),
            StructField(OtherFields.IS_FETCHED_STAT, BooleanType(), nullable=False),
            StructField(OtherFields.FETCH_STAT_TIME, TimestampType(), nullable=True)
        ])
