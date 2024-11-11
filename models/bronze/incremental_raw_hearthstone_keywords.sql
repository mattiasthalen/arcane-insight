MODEL (
  name bronze_sqlmesh.incremental_raw_hearthstone_keywords,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_loaded_at,
  ),
  columns (
    id BIGINT,
    slug TEXT,
    name TEXT,
    ref_text TEXT,
    text TEXT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    _dlt_loaded_at TIMESTAMP
  ),
);

SELECT
  id,
  slug,
  name,
  ref_text,
  text,
  _dlt_load_id,
  _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_loaded_at
FROM
  bronze.raw_hearthstone_keywords
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
