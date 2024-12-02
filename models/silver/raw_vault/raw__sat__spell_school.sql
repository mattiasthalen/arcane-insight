/* Data vault satellite model for the Hearthstone spell schools */
MODEL (
  name silver.raw_vault.raw__sat__spell_school,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key hash_key__spell_school_bk,
    columns [_sqlmesh__hash_diff],
    valid_to_name _sqlmesh__valid_to,
    valid_from_name _sqlmesh__valid_from
  ),
  allow_partials TRUE
);

SELECT
  hash_key__spell_school_bk,
  slug,
  id,
  name,
  gameModes,
  _sqlmesh__hash_diff,
  _sqlmesh__record_source,
  _sqlmesh__extracted_at,
  _sqlmesh__loaded_at
FROM silver.staging.dv_stg__hearthstone__spell_schools