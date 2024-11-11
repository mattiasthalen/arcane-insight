MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _sqlmesh_hashkey
  ),
  columns (
    id BIGINT,
    slug TEXT,
    name TEXT,
    ref_text TEXT,
    text TEXT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    _dlt_loaded_at TIMESTAMP,
    _sqlmesh_hashkey BLOB,
    _sqlmesh_loaded_at TIMESTAMP
  )
);

SELECT
  id,
  slug,
  name,
  ref_text,
  text,
  _dlt_load_id,
  _dlt_id,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_loaded_at,
  @generate_surrogate_key(id, slug, name, ref_text, text, _dlt_id) AS _sqlmesh_hashkey,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw_hearthstone_keywords /* WHERE */ /*   TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds */