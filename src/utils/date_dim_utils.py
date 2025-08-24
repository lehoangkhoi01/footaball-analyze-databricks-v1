from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, StringType
from pyspark.sql import SparkSession, DataFrame
from src.schemas.fields import DateFields

class DateUtils:
    @staticmethod
    def get_season_for_date(input_date):
        """
        Generate season string for a single date.
        Season runs from August 1st to July 31st of the following year.
        """
        if input_date is None:
            return None
            
        year = input_date.year
        month = input_date.month
        
        # If month is August or later, season starts this year
        if month >= 8:
            start_year = year
            end_year = year + 1
        else:
            # If month is January to July, season started previous year
            start_year = year - 1
            end_year = year
        
        return f"{start_year}-{str(end_year)[2:]}"

    @staticmethod
    def generate_date_df(spark: SparkSession, start_date: datetime, end_date: datetime, season: str) -> DataFrame:
        # Generate date range
        date_list = []
        current = start_date
        
        while current <= end_date:
            date_list.append((current,))
            current += timedelta(days=1)

        # Create DataFrame
        date_df = spark.createDataFrame(date_list, ["date"])

        # Create season UDF
        season_udf = udf(DateUtils.get_season_for_date, StringType())

        dim_date = date_df.select(
            # Surrogate key
            date_format(col("date"), "yyyyMMdd").cast("int").alias(DateFields.DATE_KEY),
            # Date attributes
            col("date").alias(DateFields.DATE).cast(DateType()),
            year(col("date")).alias(DateFields.YEAR),
            month(col("date")).alias(DateFields.MONTH),
            dayofmonth(col("date")).alias(DateFields.DAY_OF_MONTH),
            dayofweek(col("date")).alias(DateFields.DAY_OF_WEEK),
            dayofyear(col("date")).alias(DateFields.DAY_OF_YEAR),
            weekofyear(col("date")).alias(DateFields.WEEK_OF_YEAR),
            quarter(col("date")).alias(DateFields.QUARTER),
            # Formatted strings
            date_format(col("date"), "MMMM").alias(DateFields.MONTH_NAME),
            date_format(col("date"), "MMM").alias(DateFields.MONTH_NAME_SHORT),
            date_format(col("date"), "EEEE").alias(DateFields.DAY_NAME),
            date_format(col("date"), "EEE").alias(DateFields.DAY_NAME_SHORT),
            # Business flags
            when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False).alias(DateFields.IS_WEEKEND),
            when(dayofweek(col("date")).isin([2, 3, 4, 5, 6]), 'Weekday').otherwise('Weekend').alias(DateFields.MATCH_DAY_CATEGORY),
            # Season column - dynamically generated based on each date
            season_udf(col("date")).alias(DateFields.SEASON),
        )

        return dim_date

    @staticmethod
    def generate_date_df_with_season_sql(spark: SparkSession, start_date: datetime, end_date: datetime) -> DataFrame:
        # Generate date range
        date_list = []
        current = start_date
        
        while current <= end_date:
            date_list.append((current,))
            current += timedelta(days=1)
        
        # Create DataFrame
        date_df = spark.createDataFrame(date_list, ["date"])
        
        # Register the DataFrame as a temporary view
        date_df.createOrReplaceTempView("temp_dates")
        
        # Use SQL to generate the season column
        dim_date = spark.sql("""
            SELECT 
                -- Surrogate key
                CAST(date_format(date, 'yyyyMMdd') AS INT) AS DATE_KEY,
                -- Date attributes
                CAST(date AS DATE) AS DATE,
                year(date) AS YEAR,
                month(date) AS MONTH,
                day(date) AS DAY_OF_MONTH,
                dayofweek(date) AS DAY_OF_WEEK,
                dayofyear(date) AS DAY_OF_YEAR,
                weekofyear(date) AS WEEK_OF_YEAR,
                quarter(date) AS QUARTER,
                -- Season column using CASE WHEN logic
                CASE 
                    WHEN month(date) >= 8 THEN 
                        CONCAT(year(date), '-', LPAD(year(date) + 1 - 2000, 2, '0'))
                    ELSE 
                        CONCAT(year(date) - 1, '-', LPAD(year(date) - 2000, 2, '0'))
                END AS SEASON,
                -- Formatted strings
                date_format(date, 'MMMM') AS MONTH_NAME,
                date_format(date, 'MMM') AS MONTH_NAME_SHORT,
                date_format(date, 'EEEE') AS DAY_NAME,
                date_format(date, 'EEE') AS DAY_NAME_SHORT,
                -- Business flags
                CASE WHEN dayofweek(date) IN (1, 7) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
                CASE WHEN dayofweek(date) IN (2, 3, 4, 5, 6) THEN 'Weekday' ELSE 'Weekend' END AS MATCH_DAY_CATEGORY
            FROM temp_dates
        """)
        
        return dim_date