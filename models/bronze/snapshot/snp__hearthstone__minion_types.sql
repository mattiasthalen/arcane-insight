/* Snapshot model of the minion types from Hearthstone */
MODEL (
  name bronze.snapshot.snp__hearthstone__minion_types,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _sqlmesh__hash_diff,
    forward_only TRUE,
    disable_restatement TRUE
  )
);

SELECT
  *,
  @generate_surrogate_key__sha_256(
    @star_v2(
      relation := raw__hearthstone__minion_types,
      exclude := _sqlmesh__extracted_at,
      select_only := TRUE
    )
  ) AS _sqlmesh__hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
FROM bronze.raw.raw__hearthstone__minion_types