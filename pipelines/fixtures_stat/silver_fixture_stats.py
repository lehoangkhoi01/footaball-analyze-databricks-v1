import dlt
from pyspark.sql import SparkSession
from src.schemas.fields import TableNames, CommonFields, FixtureStatsFields, MetadataFields
from src.schemas.fixture_stats_schema import FixtureStatsSchema
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType
from src.logging.db_logger import LoggerConfig, get_pipeline_logger

# --------------------------------------------------------------------
spark = SparkSession.getActiveSession()
spark.sql("USE SCHEMA `silver`")
raw_data_storage_location = spark.conf.get("raw_source_dir")
logger = get_pipeline_logger()


#---------------------------------------------------------------------


@dlt.table(
    name=f"silver_intermediate.{TableNames.INTERMEDIATE_SILVER_FIXTURE_STATS}",
    comment="Select, transform and enrich the bronze data for the 'fixture stat' table",
)
def intermediate_silver_fixture():
    bronze_fixture_stat_df = spark.readStream.table(f'bronze.{TableNames.BRONZE_FIXTURE_STATS}')

    pivoted_df = bronze_fixture_stat_df.groupBy("fixture_id", "stats_team_id", "ingestion_time").agg(
        max(when(col("stats_statistics_type") == "Shots on Goal",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.SHOTS_ON_GOAL),
        max(when(col("stats_statistics_type") == "Shots off Goal",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.SHOTS_OFF_GOAL),
        max(when(col("stats_statistics_type") == "Total Shots",
                 col("stats_statistics_value").cast(IntegerType()))).alias("total_shots"),
        max(when(col("stats_statistics_type") == "Blocked Shots",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.BLOCKED_SHOTS),
        max(when(col("stats_statistics_type") == "Shots insidebox",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.SHOTS_INSIDEBOX),
        max(when(col("stats_statistics_type") == "Shots outsidebox",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.SHOTS_OUTSIDEBOX),
        max(when(col("stats_statistics_type") == "Fouls",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.FOULS),
        max(when(col("stats_statistics_type") == "Corner Kicks",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.CORNER_KICKS),
        max(when(col("stats_statistics_type") == "Offsides",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.OFFSIDES),
        max(when(col("stats_statistics_type") == "Ball Possession",
                   regexp_replace(col("stats_statistics_value"), "%", "").cast(IntegerType()))).alias(FixtureStatsFields.BALL_POSSESSION),
        max(when(col("stats_statistics_type") == "Yellow Cards",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.YELLOW_CARDS),
        max(when(col("stats_statistics_type") == "Red Cards",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.RED_CARDS),
        max(when(col("stats_statistics_type") == "Goalkeeper Saves",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.GOALKEEPER_SAVES),
        round(max(when(col("stats_statistics_type") == "expected_goals",
                 col("stats_statistics_value").cast(FloatType()))), 2).alias(FixtureStatsFields.EXPECTED_GOALS_RATE),
        max(when(col("stats_statistics_type") == "Total passes",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.TOTAL_PASSES),
        max(when(col("stats_statistics_type") == "Passes accurate",
                 col("stats_statistics_value").cast(IntegerType()))).alias(FixtureStatsFields.ACCURATE_PASSES),
        max(when(col("stats_statistics_type") == "Passes %",
                regexp_replace(col("stats_statistics_value"), "%", "").cast(FloatType()))).alias(FixtureStatsFields.PASSES_SUCCESS_PERCENTAGE),
        col(MetadataFields.INGESTION_TIME)
    ).select(
        col("fixture_id").alias(CommonFields.FIXTURE_ID),
        col("stats_team_id").alias(CommonFields.TEAM_ID),
        col(FixtureStatsFields.SHOTS_ON_GOAL),
        col(FixtureStatsFields.SHOTS_OFF_GOAL),
        col(FixtureStatsFields.BLOCKED_SHOTS),
        col(FixtureStatsFields.SHOTS_INSIDEBOX),
        col(FixtureStatsFields.SHOTS_OUTSIDEBOX),
        col(FixtureStatsFields.FOULS),
        col(FixtureStatsFields.CORNER_KICKS),
        col(FixtureStatsFields.OFFSIDES),
        col(FixtureStatsFields.BALL_POSSESSION),
        col(FixtureStatsFields.YELLOW_CARDS),
        col(FixtureStatsFields.RED_CARDS),
        col(FixtureStatsFields.GOALKEEPER_SAVES),
        round(col(FixtureStatsFields.EXPECTED_GOALS_RATE), 2).alias(FixtureStatsFields.EXPECTED_GOALS_RATE),
        col(FixtureStatsFields.TOTAL_PASSES),
        col(FixtureStatsFields.ACCURATE_PASSES),
        col(FixtureStatsFields.PASSES_SUCCESS_PERCENTAGE),
        col(MetadataFields.INGESTION_TIME)
    ).fillna({
        FixtureStatsFields.YELLOW_CARDS: 0,
        FixtureStatsFields.RED_CARDS: 0,
        FixtureStatsFields.OFFSIDES: 0,
        FixtureStatsFields.FOULS: 0,
        FixtureStatsFields.CORNER_KICKS: 0
    })

    return pivoted_df

@dlt.table(
    name=f"silver_intermediate.transform_fix_stat",
    comment="Select, transform and enrich the bronze data for the 'fixture stat' table",
    table_properties={
        "quality" : "silver"
    },
    schema=FixtureStatsSchema.get_silver_schema()
)
def transform_fixture_stats():
    fixture_stats_df = spark.read.table(f'silver_intermediate.{TableNames.INTERMEDIATE_SILVER_FIXTURE_STATS}')
    return fixture_stats_df


dlt.create_streaming_table(
    name=TableNames.SILVER_FIXTURE_STATS,
    comment="Silver fixture stat table with SCD type 2",
    table_properties={"quality" : "silver"},
    partition_cols=[CommonFields.TEAM_ID, CommonFields.FIXTURE_ID]
)

dlt.create_auto_cdc_from_snapshot_flow(
    target=TableNames.SILVER_FIXTURE_STATS,
    source=f"silver_intermediate.transform_fix_stat",
    keys=[CommonFields.FIXTURE_ID, CommonFields.TEAM_ID],
    stored_as_scd_type=2
)

