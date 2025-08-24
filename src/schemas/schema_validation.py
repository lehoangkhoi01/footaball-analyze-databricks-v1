from typing import List, TypedDict
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

from src.schemas.fields import FixtureStatsFields, TableNames

class ValidationResult:
    def __init__(
        self,
        quality_issues: List[str],
        schema_issues: List[str],
        business_logic_issues: List[str],
        is_valid: bool
    ):
        self.quality_issues = quality_issues
        self.schema_issues = schema_issues
        self.business_logic_issues = business_logic_issues
        self.is_valid = is_valid

    def to_str(self) -> str:
        """
        Convert ValidationResult to a readable string format.
        """
        return (
            f"ValidationResult(\n"
            f"  is_valid: {self.is_valid},\n"
            f"  quality_issues: {self.quality_issues},\n"
            f"  schema_issues: {self.schema_issues},\n"
            f"  business_logic_issues: {self.business_logic_issues}\n"
            f")"
        )

class SchemaValidation:
    @staticmethod
    def validate_data_quality(df: DataFrame, schema: StructType) -> List[str]:
        """Run data quality checks"""
        quality_issues = []
        for field in schema.fields:
            if not field.nullable:
                null_count = df.filter(col(field.name).isNull()).count()
                if null_count > 0:
                    quality_issues.append(f"{field.name} has {null_count} null values")
        return quality_issues

    @staticmethod
    def validate_schema(
        df: DataFrame,
        expected_schema: StructType,
        table_name: str
    ) -> List[str]:
        """
        Validate dataframe schema against expected schema
        """
        validation_results: List[str] = []
        expected_cols = set(field.name for field in expected_schema.fields)
        actual_cols = set(df.columns)
        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols
        if missing_cols:
            validation_results.append(f"{table_name}: Missing columns: {missing_cols}")
        if extra_cols:
            validation_results.append(f"{table_name}: Extra columns: {extra_cols}")
        for field in expected_schema.fields:
            if field.name in df.columns:
                actual_type = dict(df.dtypes)[field.name]
                expected_type = field.dataType.simpleString()
                if actual_type != expected_type:
                    validation_results.append(
                        f"{table_name}: Column '{field.name}': expected {expected_type}, got {actual_type}"
                    )
        return validation_results

    @staticmethod
    def validate_fixture_stats_fields(df: DataFrame, table_name: str) -> List[str]:
        business_logic_issues = []
        positive_value_columns = [
            FixtureStatsFields.SHOTS_ON_GOAL,
            FixtureStatsFields.SHOTS_OFF_GOAL,
            FixtureStatsFields.BLOCKED_SHOTS,
            FixtureStatsFields.SHOTS_INSIDEBOX,
            FixtureStatsFields.SHOTS_OUTSIDEBOX,
            FixtureStatsFields.FOULS,
            FixtureStatsFields.CORNER_KICKS,
            FixtureStatsFields.BALL_POSSESSION,
            FixtureStatsFields.YELLOW_CARDS,
            FixtureStatsFields.RED_CARDS,
            FixtureStatsFields.GOALKEEPER_SAVES,
            FixtureStatsFields.TOTAL_PASSES,
            FixtureStatsFields.ACCURATE_PASSES,
            FixtureStatsFields.PASSES_SUCCESS_PERCENTAGE
        ]
        for col_name in positive_value_columns:
            if col_name in df.columns:
                negative_count = df.filter(col(col_name) < 0).count()
                if negative_count > 0:
                    business_logic_issues.append(
                        f"{table_name}: '{col_name}' has {negative_count} negative values"
                    )
            else:
                business_logic_issues.append(
                    f"{table_name}: Column '{col_name}' is missing from the DataFrame"
                )
        shots_mismatch = df.filter(
            col(FixtureStatsFields.SHOTS_ON_GOAL).isNotNull() &
            col(FixtureStatsFields.SHOTS_OFF_GOAL).isNotNull() &
            col(FixtureStatsFields.BLOCKED_SHOTS).isNotNull() &
            col(FixtureStatsFields.SHOTS_INSIDEBOX).isNotNull() &
            col(FixtureStatsFields.SHOTS_OUTSIDEBOX).isNotNull()
        ).filter(
            (col(FixtureStatsFields.SHOTS_ON_GOAL) +
             col(FixtureStatsFields.SHOTS_OFF_GOAL) +
             col(FixtureStatsFields.BLOCKED_SHOTS)) !=
            (col(FixtureStatsFields.SHOTS_INSIDEBOX) +
             col(FixtureStatsFields.SHOTS_OUTSIDEBOX))
        ).count()
        if shots_mismatch > 0:
            business_logic_issues.append(
                f"Found {shots_mismatch} records where shot totals don't match (on_goal + off_goal + blocked ≠ inside_box + outside_box)"
            )
        percentage_fields = [
            FixtureStatsFields.BALL_POSSESSION,
            FixtureStatsFields.PASSES_SUCCESS_PERCENTAGE
        ]
        for field in percentage_fields:
            if field in df.columns:
                invalid_percentage_count = df.filter(
                    (col(field) < 0) | (col(field) > 100)
                ).count()
                if invalid_percentage_count > 0:
                    business_logic_issues.append(
                        f"{table_name}: '{field}' has {invalid_percentage_count} values outside the range 0-100"
                    )
        invalid_expected_goals_rate_count = df.filter(
            (col(FixtureStatsFields.EXPECTED_GOALS_RATE) < 0) |
            (col(FixtureStatsFields.EXPECTED_GOALS_RATE) > 1)
        ).count()
        if invalid_expected_goals_rate_count > 0:
            business_logic_issues.append(
                f"{table_name}: 'expected_goals_rate' has {invalid_expected_goals_rate_count} values outside the range 0-1"
            )
        if FixtureStatsFields.EXPECTED_GOALS_RATE in df.columns:
            negative_count = df.filter(col(FixtureStatsFields.EXPECTED_GOALS_RATE) < 0).count()
            if negative_count > 0:
                business_logic_issues.append(
                    f"{table_name}: {FixtureStatsFields.EXPECTED_GOALS_RATE} has {negative_count} negative values"
                )
        return business_logic_issues

    @staticmethod
    def validate_business_logic(df: DataFrame, table_name: str) -> List[str]:
        """
        Validate business logic rules specific to the table.
        """
        if table_name == TableNames.SILVER_FIXTURE_STATS:
            return SchemaValidation.validate_fixture_stats_fields(df, table_name)
        return []

    @staticmethod
    def validate_schema_and_data_quality(
        df: DataFrame,
        schema: StructType,
        table_name: str
    ) -> ValidationResult:
        quality_issues = SchemaValidation.validate_data_quality(df, schema)
        schema_issues = SchemaValidation.validate_schema(df, schema, table_name)
        business_logic_issues = SchemaValidation.validate_business_logic(df, table_name)
        is_valid = (
            len(quality_issues) == 0 and
            len(schema_issues) == 0 and
            len(business_logic_issues) == 0
        )
        result = ValidationResult(
            quality_issues=quality_issues,
            schema_issues=schema_issues,
            business_logic_issues=business_logic_issues,
            is_valid=is_valid
        )
        return result