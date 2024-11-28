/* Data vault effectivity satellite model for the Hearthstone cards & classes */
MODEL (
  name silver.raw_vault.raw__sat__link__card__class,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key hash_key__card_bk__class_bk,
    columns [hash_key__card_bk__class_bk],
    valid_to_name _sqlmesh__effective_to,
    valid_from_name _sqlmesh__effective_from
  ),
  allow_partials TRUE
);

SELECT
  hash_key__card_bk__class_bk,
  hash_key__card_bk,
  hash_key__class_bk,
  _sqlmesh__record_source,
  _sqlmesh__extracted_at,
  _sqlmesh__loaded_at
FROM silver.staging.dv_stg__hearthstone__cards