MODEL (
  name battle_net_sqlmesh.incremental_raw_hearthstone_minion_types,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  columns (
    slug TEXT,
    id BIGINT,
    name TEXT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    _dlt_load_time TIMESTAMP
  ),
);

SELECT
  slug,
  id,
  name,
  _dlt_load_id,
  _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  battle_net.raw_hearthstone_minion_types
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
