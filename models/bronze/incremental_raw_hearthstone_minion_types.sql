MODEL (
  name bronze_sqlmesh.incremental_raw_hearthstone_minion_types,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_loaded_at,
  ),
  columns (
    slug TEXT,
    id BIGINT,
    name TEXT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    _dlt_loaded_at TIMESTAMP
  ),
);

SELECT
  slug,
  id,
  name,
  _dlt_load_id,
  _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_loaded_at
FROM
  bronze.raw_hearthstone_minion_types
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
