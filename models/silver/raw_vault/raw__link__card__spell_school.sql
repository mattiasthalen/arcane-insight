/* Data vault link model for the Hearthstone cards & spell schools */
MODEL (
  name silver.raw_vault.raw__link__card__spell_school,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key hash_key__card_bk__spell_school_bk,
    columns [hash_key__card_bk__spell_school_bk],
    execution_time_as_valid_from TRUE,
    disable_restatement FALSE,
    valid_to_name _sqlmesh__valid_to,
    valid_from_name _sqlmesh__valid_from
  ),
  allow_partials TRUE
);

SELECT
  hash_key__card_bk__spell_school_bk,
  hash_key__card_bk,
  hash_key__spell_school_bk,
  _sqlmesh__record_source,
  _sqlmesh__extracted_at,
  _sqlmesh__loaded_at
FROM silver.staging.dv_stg__hearthstone__cards