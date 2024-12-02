/* Data vault hub model for the Hearthstone card types */
MODEL (
  name silver.raw_vault.raw__hub__card_type,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key hash_key__card_type_bk
  ),
  columns (
    card_type_bk TEXT,
    hash_key__card_type_bk TEXT,
    _sqlmesh__record_source TEXT,
    _sqlmesh__extracted_at TIMESTAMP,
    _sqlmesh__loaded_at TIMESTAMP
  )
);

WITH business_keys AS (
  SELECT
    0 AS source,
    card_type_bk,
    hash_key__card_type_bk,
    _sqlmesh__record_source,
    _sqlmesh__extracted_at,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__types
  UNION ALL
  SELECT
    1 AS source,
    card_type_id AS card_type_bk,
    hash_key__card_type_id AS hash_key__card_type_bk,
    _sqlmesh__record_source,
    _sqlmesh__extracted_at,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__types
  UNION ALL
  SELECT
    2 AS source,
    card_type_bk,
    hash_key__card_type_bk,
    _sqlmesh__record_source,
    _sqlmesh__extracted_at,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__cards
), deduplicated AS (
  SELECT
    *
    EXCLUDE (source)
  FROM business_keys
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY card_type_bk ORDER BY source, _sqlmesh__loaded_at) = 1
)
SELECT
  *
FROM deduplicated
ANTI JOIN silver.raw_vault.raw__hub__card_type
  USING (hash_key__card_type_bk)