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
    metadata_repo: MetadataRepository

    def load(
        self,
        source_table: str,
        stage_table: str,
        mode: LoadMode,
        incremental_column: str | None = None,
    ) -> DataFrame:
        source_df = self.spark.table(source_table)
        pipeline_cfg = self.metadata_repo.get_stage_pipeline_config(source_table, stage_table)
        resolved_incremental_column = incremental_column or pipeline_cfg.incremental_column

        if mode == "init":
            source_df.write.mode("overwrite").saveAsTable(stage_table)
            self._save_watermark_if_configured(
                source_df=source_df,
                source_table=source_table,
                stage_table=stage_table,
                pipeline_id=pipeline_cfg.pipeline_id,
                incremental_column=resolved_incremental_column,
            )
            return source_df

        if mode == "inc":
            if not resolved_incremental_column:
                raise ValueError(
                    "incremental_column is required for inc mode (from arg or etl_meta.table_mapping)"
                )

            last_watermark = self.metadata_repo.get_last_watermark(
                pipeline_id=pipeline_cfg.pipeline_id,
                incremental_column=resolved_incremental_column,
            )
            inc_df = self._apply_incremental_filter(
                source_df=source_df,
                incremental_column=resolved_incremental_column,
                last_watermark_value=last_watermark,
            )
            inc_df.write.mode("append").saveAsTable(stage_table)
            self._save_watermark_if_configured(
                source_df=inc_df,
                source_table=source_table,
                stage_table=stage_table,
                pipeline_id=pipeline_cfg.pipeline_id,
                incremental_column=resolved_incremental_column,
            )
            return inc_df

        raise ValueError("mode must be either 'init' or 'inc'")

    @staticmethod
    def _apply_incremental_filter(
        source_df: DataFrame,
        incremental_column: str,
        last_watermark_value: str | None,
    ) -> DataFrame:
        if last_watermark_value is None:
            return source_df
        return source_df.filter(F.col(incremental_column) > F.lit(last_watermark_value))

    def _save_watermark_if_configured(
        self,
        source_df: DataFrame,
        source_table: str,
        stage_table: str,
        pipeline_id: int,
        incremental_column: str | None,
    ) -> None:
        if not incremental_column:
            return

        max_value_row = (
            source_df.select(F.max(F.col(incremental_column)).cast("string").alias("max_value"))
            .collect()[0]
        )
        loaded_row_count = source_df.count()
        self.metadata_repo.save_watermark(
            pipeline_id=pipeline_id,
            source_table=source_table,
            stage_table=stage_table,
            incremental_column=incremental_column,
            loaded_max_value=max_value_row["max_value"],
            loaded_row_count=loaded_row_count,
        )


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
