MODEL (
  name bronze_sqlmesh.incremental__dlt_loads,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_loaded_at,
  ),
  columns (
    load_id TEXT,
    schema_name TEXT,
    status BIGINT,
    inserted_at TIMESTAMP,
    schema_version_hash TEXT,
    _dlt_loaded_at TIMESTAMP
  ),
);

SELECT
  load_id,
  schema_name,
  status,
  inserted_at,
  schema_version_hash,
  TO_TIMESTAMP(CAST(load_id AS DOUBLE)) as _dlt_loaded_at
FROM
  bronze._dlt_loads
WHERE
  TO_TIMESTAMP(CAST(load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
