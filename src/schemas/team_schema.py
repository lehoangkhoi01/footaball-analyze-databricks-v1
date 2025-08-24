from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, DateType, TimestampType,
    ArrayType, LongType
)

from src.schemas.fields import CommonFields, TeamFields, MetadataFields

class TeamSchema:
    @staticmethod
    def get_bronze_schema():
        return StructType([
            StructField("team", StructType([
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=False),
                StructField("code", StringType(), nullable=True),
                StructField("country", StringType(), nullable=True),
                StructField("founded", IntegerType(), nullable=True),
                StructField("national", BooleanType(), nullable=True),
                StructField("logo", StringType(), nullable=True),
            ]), nullable=False),

            StructField("venue", StructType([
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("address", StringType(), nullable=True),
                StructField("city", StringType(), nullable=True),
                StructField("capacity", IntegerType(), nullable=True),
                StructField("image", StringType(), nullable=True),
                StructField("surface", StringType(), nullable=True),
            ]), nullable=True),
        ])
    
    @staticmethod 
    def get_silver_schema():
        return StructType([
            StructField(CommonFields.TEAM_ID, LongType(), nullable=False),
            StructField(TeamFields.TEAM_NAME, StringType(), nullable=False),
            StructField(TeamFields.CODE, StringType(), nullable=True),
            StructField(TeamFields.COUNTRY, StringType(), nullable=True),
            StructField(TeamFields.FOUNDED, IntegerType(), nullable=True),
            StructField(TeamFields.NATIONAL, BooleanType(), nullable=True),
            StructField(TeamFields.LOGO, StringType(), nullable=True),
            StructField(TeamFields.STADIUM_ID, LongType(), nullable=True),
            StructField(TeamFields.STADIUM_NAME, StringType(), nullable=True),
            StructField(TeamFields.STADIUM_ADDRESS, StringType(), nullable=True),
            StructField(TeamFields.CITY, StringType(), nullable=True),
            StructField(TeamFields.CAPACITY, LongType(), nullable=True),
            StructField(TeamFields.STADIUM_IMAGE, StringType(), nullable=True),
            StructField(TeamFields.STADIUM_SURFACE, StringType(), nullable=True),
            StructField(MetadataFields.INGESTION_TIME, TimestampType(), nullable=True)
        ])
    
    @staticmethod
    def get_gold_schema():
        return StructType([
            StructField(TeamFields.TEAM_KEY, LongType(), nullable=False),
            StructField(CommonFields.TEAM_ID, LongType(), nullable=False),
            StructField(TeamFields.TEAM_NAME, StringType(), nullable=False),
            StructField(TeamFields.CODE, StringType(), nullable=True),
            StructField(TeamFields.COUNTRY, StringType(), nullable=True),
            StructField(TeamFields.FOUNDED, IntegerType(), nullable=True),
            StructField(TeamFields.NATIONAL, BooleanType(), nullable=True),
            StructField(TeamFields.LOGO, StringType(), nullable=True),
            StructField(TeamFields.STADIUM_ID, LongType(), nullable=True),
            StructField(TeamFields.STADIUM_NAME, StringType(), nullable=True),
            StructField(TeamFields.STADIUM_ADDRESS, StringType(), nullable=True),
            StructField(TeamFields.CITY, StringType(), nullable=True),
            StructField(TeamFields.CAPACITY, LongType(), nullable=True),
            StructField(TeamFields.STADIUM_IMAGE, StringType(), nullable=True),
            StructField(TeamFields.STADIUM_SURFACE, StringType(), nullable=True),

            # Derived attributes
            StructField(TeamFields.TEAM_AGE, IntegerType(), nullable=True),
            StructField(TeamFields.CAPACITY_CATEGORY, StringType(), nullable=False),

            # Metadata
            # StructField(CommonFields.CREATED_AT, TimestampType(), nullable=False),
            # StructField(CommonFields.UPDATED_AT, TimestampType(), nullable=False),

            # # SCD fields
            # StructField(CommonFields.EFFECTIVE_FROM, DateType(), nullable=False),
            # StructField(CommonFields.EFFECTIVE_TO, DateType(), nullable=True),
            # StructField(CommonFields.IS_CURRENT, BooleanType(), nullable=False)
        ])