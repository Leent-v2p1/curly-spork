-- Metadata schema for source->target resolution and column-level mappings.
CREATE DATABASE IF NOT EXISTS etl_meta;

CREATE TABLE IF NOT EXISTS etl_meta.table_mapping (
    pipeline_id BIGINT,
    source_table STRING,
    stage_table STRING,
    target_table STRING,
    load_type STRING,          -- e.g. full/inc
    incremental_column STRING, -- used by source->stage in inc mode
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING PARQUET;

CREATE TABLE IF NOT EXISTS etl_meta.column_mapping (
    pipeline_id BIGINT,
    column_order INT,
    source_column STRING,
    target_column STRING,
    transform_expr STRING, -- optional spark SQL expression (e.g. "upper(name)")
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING PARQUET;

CREATE TABLE IF NOT EXISTS etl_meta.load_stats (
    pipeline_id BIGINT,
    source_table STRING,
    stage_table STRING,
    incremental_column STRING,
    loaded_max_value STRING, -- max loaded value serialized to string
    loaded_row_count BIGINT,
    load_finished_at TIMESTAMP
)
USING PARQUET;
