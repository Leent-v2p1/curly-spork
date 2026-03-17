from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F


@dataclass(frozen=True)
class ColumnMapping:
    source_column: str
    target_column: str
    transform_expr: str | None = None


class MetadataRepository:
    """Reads ETL metadata stored in Spark tables."""

    def __init__(
        self,
        spark: SparkSession,
        table_mapping_table: str = "etl_meta.table_mapping",
        column_mapping_table: str = "etl_meta.column_mapping",
    ) -> None:
        self.spark = spark
        self.table_mapping_table = table_mapping_table
        self.column_mapping_table = column_mapping_table

    def resolve_pipeline_id(self, source_table: str, target_table: str) -> int:
        row = (
            self.spark.table(self.table_mapping_table)
            .filter(
                (F.col("source_table") == source_table)
                & (F.col("target_table") == target_table)
                & (F.col("is_active") == F.lit(True))
            )
            .select("pipeline_id")
            .limit(1)
            .collect()
        )
        if not row:
            raise ValueError(
                f"No active table mapping found for source={source_table} target={target_table}"
            )
        return int(row[0]["pipeline_id"])

    def get_column_mappings(self, source_table: str, target_table: str) -> List[ColumnMapping]:
        pipeline_id = self.resolve_pipeline_id(source_table, target_table)
        rows = (
            self.spark.table(self.column_mapping_table)
            .filter(
                (F.col("pipeline_id") == pipeline_id)
                & (F.col("is_active") == F.lit(True))
            )
            .orderBy(F.col("column_order").asc())
            .select("source_column", "target_column", "transform_expr")
            .collect()
        )
        if not rows:
            raise ValueError(
                f"No active column mappings found for pipeline_id={pipeline_id} ({source_table} -> {target_table})"
            )
        return [
            ColumnMapping(
                source_column=r["source_column"],
                target_column=r["target_column"],
                transform_expr=r["transform_expr"],
            )
            for r in rows
        ]

    def as_dataframe(self, source_table: str, target_table: str) -> DataFrame:
        """Return raw mapping rows for debugging/auditing."""
        pipeline_id = self.resolve_pipeline_id(source_table, target_table)
        return self.spark.table(self.column_mapping_table).filter(
            (F.col("pipeline_id") == pipeline_id) & (F.col("is_active") == F.lit(True))
        )
