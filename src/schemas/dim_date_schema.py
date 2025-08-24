from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, DateType
)

from src.schemas.fields import DateFields

class DimDateSchema:
    @staticmethod
    def get_schema():
        return StructType([
            StructField(DateFields.DATE_KEY, IntegerType(), nullable=False),
            StructField(DateFields.DATE, DateType(), nullable=False),
            StructField(DateFields.YEAR, IntegerType(), nullable=False),
            StructField(DateFields.MONTH, IntegerType(), nullable=False),
            StructField(DateFields.MONTH_NAME, StringType(), nullable=False),
            StructField(DateFields.QUARTER, IntegerType(), nullable=False),
            StructField(DateFields.WEEK_OF_YEAR, IntegerType(), nullable=False),
            StructField(DateFields.DAY_OF_MONTH, IntegerType(), nullable=False),
            StructField(DateFields.DAY_OF_WEEK, IntegerType(), nullable=False),
            StructField(DateFields.DAY_OF_YEAR, IntegerType(), nullable=False),
            StructField(DateFields.IS_WEEKEND, BooleanType(), nullable=False),
            StructField(DateFields.DAY_NAME, StringType(), nullable=False),
            StructField(DateFields.DAY_NAME_SHORT, StringType(), nullable=True),
            StructField(DateFields.MONTH_NAME_SHORT, StringType(), nullable=False),
            StructField(DateFields.SEASON, StringType(), nullable=True),
            StructField(DateFields.MATCH_DAY_CATEGORY, StringType(), nullable=True),
        ])