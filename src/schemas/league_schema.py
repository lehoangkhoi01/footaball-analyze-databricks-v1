from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, DateType, TimestampType,
    ArrayType, LongType,
)

from src.schemas.fields import CommonFields, LeagueFields

class LeagueSchema:
    @staticmethod
    def get_bronze_schema():
        return StructType([
            StructField("league", StructType([
                StructField("id", LongType(), nullable=False),
                StructField("name", StringType(), False),
                StructField("type", StringType(), False),
                StructField("logo", StringType(), True),
            ]), nullable=False),

            StructField("season", ArrayType(StructType([
                StructField("year", IntegerType(), nullable=False),
                StructField("start", StringType(), nullable=False),
                StructField("end", StringType(), nullable=False),
                StructField("current", BooleanType(), nullable=False),
                StructField("coverage", StructType([
                    StructField("fixtures", StructType([
                        StructField("events", BooleanType(), nullable=True),
                        StructField("lineups", BooleanType(), nullable=True),
                        StructField("statistics_fixtures", BooleanType(), nullable=True),
                        StructField("statistics_players", BooleanType(), nullable=True),
                    ]), nullable=True),
                    StructField("standings", BooleanType(), nullable=True),
                    StructField("players", BooleanType(), nullable=True),
                    StructField("top_scorers", BooleanType(), nullable=True),
                    StructField("top_assists", BooleanType(), nullable=True),
                    StructField("top_cards", BooleanType(), nullable=True),
                    StructField("injuries", BooleanType(), nullable=True),
                    StructField("predictions", BooleanType(), nullable=True),
                    StructField("odds", BooleanType(), nullable=True),
                ]), nullable=True),
            ])), nullable=False),

            StructField("country", StructType([
                StructField("name", StringType(), nullable=True),
                StructField("code", StringType(), True),
                StructField("flag", StringType(), True),
            ]), nullable=False),
        ])
    
    @staticmethod
    def get_silver_schema():
        return StructType([
            StructField(CommonFields.LEAGUE_ID, LongType(), nullable=False),
            StructField(LeagueFields.LEAGUE_NAME, StringType(), nullable=False),
            StructField(LeagueFields.TYPE, StringType(), nullable=False),
            StructField(LeagueFields.LOGO, StringType(), nullable=True),
            StructField(LeagueFields.COUNTRY, StringType(), nullable=True),
            StructField(LeagueFields.COUNTRY_FLAG, StringType(), nullable=True)
        ])

    @staticmethod
    def get_gold_schema():
        return StructType([
            StructField(LeagueFields.LEAGUE_KEY, LongType(), nullable=False),
            StructField(CommonFields.LEAGUE_ID, LongType(), nullable=False),
            StructField(LeagueFields.LEAGUE_NAME, StringType(), nullable=False),
            StructField(LeagueFields.TYPE, StringType(), nullable=False),
            StructField(LeagueFields.LOGO, StringType(), nullable=True),
            StructField(LeagueFields.COUNTRY, StringType(), nullable=True),
            StructField(LeagueFields.COUNTRY_FLAG, StringType(), nullable=True)
            
            # Metadata fields
            # StructField(CommonFields.CREATED_AT, TimestampType(), nullable=False),
            # StructField(CommonFields.UPDATED_AT, TimestampType(), nullable=False)
        ])
    