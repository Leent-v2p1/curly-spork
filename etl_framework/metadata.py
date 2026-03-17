from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F


@dataclass(frozen=True)
class ColumnMapping:
    source_column: str
    target_column: str
    transform_expr: str | None = None


@dataclass(frozen=True)
class StagePipelineConfig:
    pipeline_id: int
    incremental_column: str | None


class MetadataRepository:
    """Reads ETL metadata stored in Spark tables."""

    def __init__(
        self,
        spark: SparkSession,
        table_mapping_table: str = "etl_meta.table_mapping",
        column_mapping_table: str = "etl_meta.column_mapping",
        load_stats_table: str = "etl_meta.load_stats",
    ) -> None:
        self.spark = spark
        self.table_mapping_table = table_mapping_table
        self.column_mapping_table = column_mapping_table
        self.load_stats_table = load_stats_table

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

    def get_stage_pipeline_config(self, source_table: str, stage_table: str) -> StagePipelineConfig:
        row = (
            self.spark.table(self.table_mapping_table)
            .filter(
                (F.col("source_table") == source_table)
                & (F.col("stage_table") == stage_table)
                & (F.col("is_active") == F.lit(True))
            )
            .select("pipeline_id", "incremental_column")
            .limit(1)
            .collect()
        )
        if not row:
            raise ValueError(
                f"No active stage mapping found for source={source_table} stage={stage_table}"
            )

        return StagePipelineConfig(
            pipeline_id=int(row[0]["pipeline_id"]),
            incremental_column=row[0]["incremental_column"],
        )

    def get_last_watermark(self, pipeline_id: int, incremental_column: str) -> str | None:
        rows = (
            self.spark.table(self.load_stats_table)
            .filter(
                (F.col("pipeline_id") == pipeline_id)
                & (F.col("incremental_column") == incremental_column)
                & F.col("loaded_max_value").isNotNull()
            )
            .orderBy(F.col("load_finished_at").desc())
            .select("loaded_max_value")
            .limit(1)
            .collect()
        )
        if not rows:
            return None
        return rows[0]["loaded_max_value"]

    def save_watermark(
        self,
        pipeline_id: int,
        source_table: str,
        stage_table: str,
        incremental_column: str,
        loaded_max_value: str | None,
        loaded_row_count: int,
    ) -> None:
        now = datetime.utcnow()
        payload = [
            {
                "pipeline_id": pipeline_id,
                "source_table": source_table,
                "stage_table": stage_table,
                "incremental_column": incremental_column,
                "loaded_max_value": loaded_max_value,
                "loaded_row_count": loaded_row_count,
                "load_finished_at": now,
            }
        ]
        self.spark.createDataFrame(payload).write.mode("append").saveAsTable(self.load_stats_table)

    def get_column_mappings(self, source_table: str, target_table: str) -> List[ColumnMapping]:
        pipeline_id = self.resolve_pipeline_id(source_table, target_table)
        rows = (
            self.spark.table(self.column_mapping_table)
            .filter((F.col("pipeline_id") == pipeline_id) & (F.col("is_active") == F.lit(True)))
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
