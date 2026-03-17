# curly-spork

Simple metadata-driven ETL framework over PySpark.

## What is included

- `SourceToStageLoader`: loads source table to stage table **as is**.
  - `init` mode: overwrite stage from source.
  - `inc` mode: reads the last saved watermark from metadata and loads only new rows.
  - Stores a new watermark (`max(incremental_column)`) in metadata after every successful load.
- `StageToTargetLoader`: loads stage to target table using metadata-defined source->target column mappings.
- `MetadataRepository`: resolves pipeline/table relationships, mappings, and load stats.
- `sql/metadata_tables.sql`: metadata table DDL.

## Metadata model

Three metadata tables are used:

1. `etl_meta.table_mapping`
   - Links source/stage/target tables through `pipeline_id`.
   - Stores incremental load hints (`incremental_column`).
2. `etl_meta.column_mapping`
   - Defines column mapping from source column to target column.
   - Supports optional transform expression (`transform_expr`) using Spark SQL expressions.
3. `etl_meta.load_stats`
   - Stores watermark history for incremental loads.
   - `SourceToStageLoader` reads latest watermark in `inc` mode and writes a new one after load.

## Quick usage

```python
from pyspark.sql import SparkSession

from etl_framework import MetadataRepository, SourceToStageLoader, StageToTargetLoader

spark = SparkSession.builder.getOrCreate()

metadata = MetadataRepository(spark)
source_to_stage = SourceToStageLoader(spark, metadata)
stage_to_target = StageToTargetLoader(spark, metadata)

# source -> stage (incremental watermark read from etl_meta.load_stats)
source_to_stage.load(
    source_table="raw.customer",
    stage_table="stg.customer",
    mode="inc",
)

# source -> stage (explicitly override incremental column if needed)
source_to_stage.load(
    source_table="raw.customer",
    stage_table="stg.customer",
    mode="inc",
    incremental_column="updated_at",
)

# stage -> target
stage_to_target.load(
    stage_table="stg.customer",
    target_table="dwh.dim_customer",
    source_table="raw.customer",
    mode="inc",
)
```
