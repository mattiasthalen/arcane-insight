MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _sqlmesh_hashkey
  ),
  columns (
    value BIGINT,
    _dlt_parent_id TEXT,
    _dlt_list_idx BIGINT,
    _dlt_id TEXT,
    _sqlmesh_hashkey BLOB,
    _sqlmesh_loaded_at TIMESTAMP
  )
);

SELECT
  value,
  _dlt_parent_id,
  _dlt_list_idx,
  _dlt_id,
  @generate_surrogate_key(value, _dlt_parent_id, _dlt_list_idx, _dlt_id) AS _sqlmesh_hashkey,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw_hearthstone_cards__keyword_ids