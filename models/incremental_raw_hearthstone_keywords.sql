MODEL (
  name battle_net_sqlmesh.incremental_raw_hearthstone_keywords,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  columns (
    id BIGINT,
    slug TEXT,
    name TEXT,
    ref_text TEXT,
    text TEXT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    _dlt_load_time TIMESTAMP
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
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  bronze.raw_hearthstone_keywords
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
