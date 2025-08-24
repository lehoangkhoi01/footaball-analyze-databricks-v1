from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

class DataUtils:
    @staticmethod
    def flatten_dataframe(df: DataFrame, separator: str = "_") -> DataFrame:
        """
        Recursively flatten a DataFrame with nested structs and arrays.

        Args:
            df: Input DataFrame with nested structures
            separator: String to separate nested field names (default: "_")

        Returns:
            Flattened DataFrame
        """
        def get_nested_columns(schema, prefix=""):
            """Extract all nested column paths from schema"""
            columns = []
            for field in schema.fields:
                field_name = f"{prefix}{separator}{field.name}" if prefix else field.name
                if isinstance(field.dataType, StructType):
                    columns.extend(get_nested_columns(field.dataType, field_name))
                elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                    columns.append((field_name, "array_struct"))
                else:
                    columns.append((field_name, "regular"))
            return columns

        def flatten_struct_columns(df, separator="_"):
            """Flatten struct columns by selecting nested fields"""
            select_exprs = []
            for field in df.schema.fields:
                if isinstance(field.dataType, StructType):
                    for nested_field in field.dataType.fields:
                        alias_name = f"{field.name}{separator}{nested_field.name}"
                        select_exprs.append(col(f"{field.name}.{nested_field.name}").alias(alias_name))
                elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                    select_exprs.append(col(field.name))
                else:
                    select_exprs.append(col(field.name))
            return df.select(*select_exprs)

        def has_nested_structs(df):
            """Check if DataFrame still has nested structs"""
            for field in df.schema.fields:
                if isinstance(field.dataType, StructType):
                    return True
                elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                    return True
            return False

        current_df = df
        max_iterations = 10
        iteration = 0

        while has_nested_structs(current_df) and iteration < max_iterations:
            array_columns = [
                field.name for field in current_df.schema.fields
                if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType)
            ]
            for array_col in array_columns:
                other_cols = [
                    col(field.name) for field in current_df.schema.fields if field.name != array_col
                ]
                exploded_df = current_df.select(
                    *other_cols,
                    explode_outer(col(array_col)).alias(f"{array_col}_exploded")
                )
                if isinstance(exploded_df.schema[f"{array_col}_exploded"].dataType, StructType):
                    struct_cols = [
                        col(f"{array_col}_exploded.{nested_field.name}").alias(f"{array_col}{separator}{nested_field.name}")
                        for nested_field in exploded_df.schema[f"{array_col}_exploded"].dataType.fields
                    ]
                    other_cols_final = [
                        col(field.name) for field in exploded_df.schema.fields if field.name != f"{array_col}_exploded"
                    ]
                    current_df = exploded_df.select(*other_cols_final, *struct_cols)
                else:
                    current_df = exploded_df.withColumnRenamed(f"{array_col}_exploded", array_col)
            current_df = flatten_struct_columns(current_df, separator)
            iteration += 1

        return current_df