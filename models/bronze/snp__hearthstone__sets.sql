MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _sqlmesh_hashkey
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
    _dlt_loaded_at TIMESTAMP,
    _sqlmesh_hashkey BLOB,
    _sqlmesh_loaded_at TIMESTAMP
  )
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
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_loaded_at,
  @generate_surrogate_key(
    id,
    name,
    slug,
    hyped,
    type,
    collectible_count,
    collectible_revealed_count,
    non_collectible_count,
    non_collectible_revealed_count,
    _dlt_id
  ) AS _sqlmesh_hashkey,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw_hearthstone_sets /* WHERE */ /*   TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds */