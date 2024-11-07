MODEL (
  name battle_net_sqlmesh.incremental_raw_hearthstone_sets,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  columns (
    id BIGINT,
    name TEXT,
    slug TEXT,
    hyped BOOLEAN,
    type TEXT,
    collectible_count BIGINT,
    collectible_revealed_count BIGINT,
    non_collectible_count BIGINT,
    non_collectible_revealed_count BIGINT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    _dlt_load_time TIMESTAMP
  ),
);

SELECT
  id,
  name,
  slug,
  hyped,
  type,
  collectible_count,
  collectible_revealed_count,
  non_collectible_count,
  non_collectible_revealed_count,
  _dlt_load_id,
  _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  battle_net.raw_hearthstone_sets
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
