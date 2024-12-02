/* Data vault satellite model for the Hearthstone card sets */
MODEL (
  name silver.raw_vault.raw__sat__card_set,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key hash_key__card_set_bk,
    columns [_sqlmesh__hash_diff],
    valid_to_name _sqlmesh__valid_to,
    valid_from_name _sqlmesh__valid_from
  ),
  allow_partials TRUE
);

SELECT
  hash_key__card_set_bk,
  id,
  name,
  slug,
  hyped,
  type,
  collectibleCount,
  collectibleRevealedCount,
  nonCollectibleCount,
  nonCollectibleRevealedCount,
  _sqlmesh__hash_diff,
  _sqlmesh__record_source,
  _sqlmesh__extracted_at,
  _sqlmesh__loaded_at
FROM silver.staging.dv_stg__hearthstone__sets