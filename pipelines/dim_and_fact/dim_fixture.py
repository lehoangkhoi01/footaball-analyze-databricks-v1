import dlt
from src.schemas.fields import TableNames, CommonFields, FixtureFields
from src.schemas.fixture_schema import FixtureSchema
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import LongType
from pyspark.sql.window import Window
from src.utils.football_utils import DataFrameFootballUtils

@dlt.table(
    name=f"gold.{TableNames.DIM_FIXTURES}",
    table_properties={"quality": "gold"},
    schema=FixtureSchema.get_dim_fixture_schema()
)
def dim_fixtures():
    silver_fixtures_df = spark.read.table(f"silver.{TableNames.SILVER_FIXTURES}")

    dim_fixture_df = (
        silver_fixtures_df
        .withColumn(
            FixtureFields.SCORE_FULLTIME,
            DataFrameFootballUtils.transfrom_score_str(
                col(FixtureFields.SCORE_FULLTIME_HOME),
                col(FixtureFields.SCORE_FULLTIME_AWAY)
            )
        )
        .withColumn(
            FixtureFields.SCORE_HALFTIME, 
            DataFrameFootballUtils.transfrom_score_str(
                col(FixtureFields.SCORE_HALFTIME_HOME),
                col(FixtureFields.SCORE_HALFTIME_AWAY)
            )
        )
        .withColumn(
            FixtureFields.SCORE_EXTRATIME, 
            DataFrameFootballUtils.transfrom_score_str(
                col(FixtureFields.SCORE_EXTRATIME_HOME),
                col(FixtureFields.SCORE_EXTRATIME_AWAY)
            )
        )
    )
    # Add surrogate key
    window_spec = Window.orderBy(CommonFields.FIXTURE_ID)
    dim_fixture_df = dim_fixture_df.withColumn(
        FixtureFields.FIXTURE_KEY,
        row_number().over(window_spec).cast(LongType())
    )
    dim_fixture_df = dim_fixture_df.drop("ingestion_time")

    return dim_fixture_df