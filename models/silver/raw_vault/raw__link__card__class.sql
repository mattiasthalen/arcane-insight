/* Data vault link model for the Hearthstone cards & classes */
MODEL (
  name silver.raw_vault.raw__link__card__class,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key hash_key__card_bk__class_bk,
    columns [hash_key__card_bk__class_bk],
    execution_time_as_valid_from TRUE,
    disable_restatement FALSE,
    valid_to_name _sqlmesh_valid_to,
    valid_from_name _sqlmesh_valid_from
  ),
  allow_partials TRUE
);

SELECT
  hash_key__card_bk__class_bk,
  hash_key__card_bk,
  hash_key__class_bk,
  _sqlmesh_record_source,
  _dlt_extracted_at,
  _sqlmesh_loaded_at
FROM silver.staging.dv_stg__hearthstone__cards