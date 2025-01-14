/* Data vault hub model for the Hearthstone rarities */
MODEL (
  name silver.raw_vault.raw__hub__rarity,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key hash_key__rarity_bk
  ),
  columns (
    rarity_bk TEXT,
    hash_key__rarity_bk TEXT,
    _sqlmesh_record_source TEXT,
    _dlt_extracted_at TIMESTAMP,
    _sqlmesh_loaded_at TIMESTAMP
  )
);

WITH business_keys AS (
  SELECT
    0 AS source,
    rarity_bk,
    hash_key__rarity_bk,
    _sqlmesh_record_source,
    _dlt_extracted_at,
    _sqlmesh_loaded_at
  FROM silver.staging.dv_stg__hearthstone__rarities
  UNION ALL
  SELECT
    1 AS source,
    rarity_id AS rarity_bk,
    hash_key__rarity_id AS hash_key__rarity_bk,
    _sqlmesh_record_source,
    _dlt_extracted_at,
    _sqlmesh_loaded_at
  FROM silver.staging.dv_stg__hearthstone__rarities
  UNION ALL
  SELECT
    2 AS source,
    rarity_bk,
    hash_key__rarity_bk,
    _sqlmesh_record_source,
    _dlt_extracted_at,
    _sqlmesh_loaded_at
  FROM silver.staging.dv_stg__hearthstone__cards
), deduplicated AS (
  SELECT
    *
    EXCLUDE (source)
  FROM business_keys
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY rarity_bk ORDER BY source, _sqlmesh_loaded_at) = 1
)
SELECT
  *
FROM deduplicated
ANTI JOIN silver.raw_vault.raw__hub__rarity
  USING (hash_key__rarity_bk)