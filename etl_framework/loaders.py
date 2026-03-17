from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pyspark.sql import DataFrame, SparkSession, functions as F

from .metadata import MetadataRepository

LoadMode = Literal["init", "inc"]


@dataclass
class SourceToStageLoader:
    """Loads source table rows to a stage table with no shape changes."""

    spark: SparkSession

    def load(
        self,
        source_table: str,
        stage_table: str,
        mode: LoadMode,
        incremental_column: str | None = None,
        last_watermark_value: str | int | None = None,
    ) -> DataFrame:
        source_df = self.spark.table(source_table)

        if mode == "init":
            source_df.write.mode("overwrite").saveAsTable(stage_table)
            return source_df

        if mode == "inc":
            inc_df = self._apply_incremental_filter(
                source_df,
                incremental_column=incremental_column,
                last_watermark_value=last_watermark_value,
            )
            inc_df.write.mode("append").saveAsTable(stage_table)
            return inc_df

        raise ValueError("mode must be either 'init' or 'inc'")

    @staticmethod
    def _apply_incremental_filter(
        source_df: DataFrame,
        incremental_column: str | None,
        last_watermark_value: str | int | None,
    ) -> DataFrame:
        if incremental_column is None:
            return source_df
        if last_watermark_value is None:
            raise ValueError("last_watermark_value is required when incremental_column is set")
        return source_df.filter(F.col(incremental_column) > F.lit(last_watermark_value))


@dataclass
class StageToTargetLoader:
    """Loads stage data to target using metadata-driven column mapping."""

    spark: SparkSession
    metadata_repo: MetadataRepository

    def load(self, stage_table: str, target_table: str, source_table: str, mode: LoadMode) -> DataFrame:
        stage_df = self.spark.table(stage_table)
        mappings = self.metadata_repo.get_column_mappings(
            source_table=source_table,
            target_table=target_table,
        )

        select_exprs = []
        for mapping in mappings:
            if mapping.transform_expr:
                select_exprs.append(F.expr(mapping.transform_expr).alias(mapping.target_column))
            else:
                select_exprs.append(F.col(mapping.source_column).alias(mapping.target_column))

        projected_df = stage_df.select(*select_exprs)

        write_mode = "overwrite" if mode == "init" else "append"
        projected_df.write.mode(write_mode).saveAsTable(target_table)
        return projected_df
