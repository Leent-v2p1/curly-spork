# curly-spork

Simple metadata-driven ETL framework over PySpark.

## What is included

- `SourceToStageLoader`: loads source table to stage table **as is**.
  - `init` mode: overwrite stage from source.
  - `inc` mode: append incremental records (optionally filtered by an incremental column + watermark).
- `StageToTargetLoader`: loads stage to target table using metadata-defined source->target column mappings.
- `MetadataRepository`: resolves pipeline/table relationships and column mapping metadata.
- `sql/metadata_tables.sql`: metadata table DDL.

## Metadata model

Two metadata tables are used:

1. `etl_meta.table_mapping`
   - Links source/stage/target tables through `pipeline_id`.
   - Stores incremental load hints (`incremental_column`).
2. `etl_meta.column_mapping`
   - Defines column mapping from source column to target column.
   - Supports optional transform expression (`transform_expr`) using Spark SQL expressions.

## Quick usage

```python
from pyspark.sql import SparkSession

from etl_framework import MetadataRepository, SourceToStageLoader, StageToTargetLoader

spark = SparkSession.builder.getOrCreate()

metadata = MetadataRepository(spark)
source_to_stage = SourceToStageLoader(spark)
stage_to_target = StageToTargetLoader(spark, metadata)

# source -> stage
source_to_stage.load(
    source_table="raw.customer",
    stage_table="stg.customer",
    mode="inc",
    incremental_column="updated_at",
    last_watermark_value="2026-01-01 00:00:00",
)

# stage -> target
stage_to_target.load(
    stage_table="stg.customer",
    target_table="dwh.dim_customer",
    source_table="raw.customer",
    mode="inc",
)
```
