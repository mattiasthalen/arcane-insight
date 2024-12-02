/* Data vault satellite model for the Hearthstone classes */
MODEL (
  name silver.raw_vault.raw__sat__class,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key hash_key__class_bk,
    columns [_sqlmesh__hash_diff],
    valid_to_name _sqlmesh__valid_to,
    valid_from_name _sqlmesh__valid_from
  ),
  allow_partials TRUE
);

SELECT
  hash_key__class_bk,
  slug,
  id,
  name,
  _sqlmesh__hash_diff,
  _sqlmesh__record_source,
  _sqlmesh__extracted_at,
  _sqlmesh__loaded_at
FROM silver.staging.dv_stg__hearthstone__classes