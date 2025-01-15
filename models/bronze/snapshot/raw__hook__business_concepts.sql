/* Snapshot model of the business concepts from the hook yaml */
MODEL (
  name bronze.snapshot.snp__hook__business_concepts,
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
    @star_v2(relation := raw__hook__business_concepts, exclude := _sqlmesh_extracted_at, select_only := TRUE)
  ) AS _sqlmesh_hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw.raw__hook__business_concepts
