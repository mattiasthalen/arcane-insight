/* Data vault hub model for the Hearthstone cards */
MODEL (
  name silver.raw_vault.raw__hub__card,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key hash_key__card_bk,
    columns [hash_key__card_bk],
    execution_time_as_valid_from TRUE,
    disable_restatement FALSE,
    valid_to_name _sqlmesh_valid_to,
    valid_from_name _sqlmesh_valid_from
  ),
  allow_partials TRUE
);

WITH business_keys AS (
  SELECT
    0 AS source,
    card_bk,
    hash_key__card_bk,
    _sqlmesh_record_source,
    _dlt_extracted_at,
    _sqlmesh_loaded_at
  FROM silver.staging.dv_stg__hearthstone__cards
  UNION ALL
  SELECT
    1 AS source,
    card_id AS card_bk,
    hash_key__card_id AS hash_key__card_bk,
    _sqlmesh_record_source,
    _dlt_extracted_at,
    _sqlmesh_loaded_at
  FROM silver.staging.dv_stg__hearthstone__cards
  UNION ALL
  SELECT
    1 AS source,
    card_bk,
    hash_key__card_bk,
    _sqlmesh_record_source,
    _dlt_extracted_at,
    _sqlmesh_loaded_at
  FROM silver.staging.dv_stg__hearthstone__classes
), deduplicated AS (
  SELECT
    *
    EXCLUDE (source)
  FROM business_keys
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY card_bk ORDER BY source, _sqlmesh_loaded_at) = 1
)
SELECT
  *
FROM deduplicated