/* Snapshot model of the rarities from Hearthstone */
MODEL (
  name bronze.snapshot.snp__hearthstone__rarities,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh_hash_diff,
    valid_from_name _sqlmesh_valid_from,
    valid_to_name _sqlmesh_valid_to,
    columns [_sqlmesh_hash_diff]
  )
);

SELECT
  *,
  @generate_surrogate_key__sha_256(
    @star_v2(
      relation := raw__hearthstone__rarities,
      exclude := _dlt_load_id,
      select_only := TRUE
    )
  ) AS _sqlmesh_hash_diff,
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw.raw__hearthstone__rarities