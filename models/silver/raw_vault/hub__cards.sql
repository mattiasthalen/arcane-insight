/* Data vault hub model for the Hearthstone cards */
MODEL (
  name silver.raw_vault.raw__hub__cards,
  kind FULL
);

WITH business_keys AS (
  SELECT
    0 AS source,
    card_bk,
    hash_key__card_bk,
    _sqlmesh__record_source,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__cards
  UNION ALL
  SELECT
    1 AS source,
    card_id AS card_bk,
    hash_key__card_id AS hash_key__card_bk,
    _sqlmesh__record_source,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__cards
  UNION ALL
  SELECT
    1 AS source,
    card_bk,
    hash_key__card_bk,
    _sqlmesh__record_source,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__classes
), deduplicated AS (
  SELECT
    *
    EXCLUDE (source)
  FROM business_keys
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY card_bk ORDER BY source, _sqlmesh__loaded_at) = 1
)
SELECT
  *
FROM deduplicated