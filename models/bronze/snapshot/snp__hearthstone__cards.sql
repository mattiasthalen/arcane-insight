/* Snapshot model of the cards from Hearthstone */
MODEL (
  name bronze.snapshot.snp__hearthstone__cards,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key _sqlmesh__hash_diff,
    valid_from_name _sqlmesh__valid_from,
    valid_to_name _sqlmesh__valid_to,
    on_destructive_change warn,
    columns [_sqlmesh__hash_diff]
  )
);

SELECT
  *,
  @generate_surrogate_key__sha_256(
    @star_v2(
      relation := raw__hearthstone__cards,
      exclude := _sqlmesh__extracted_at,
      select_only := TRUE
    )
  ) AS _sqlmesh__hash_diff,
  @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
FROM bronze.raw.raw__hearthstone__cards