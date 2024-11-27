/* Data vault hub model for the Hearthstone classes */
MODEL (
  name silver.raw_vault.raw__hub__classes,
  kind FULL
);

WITH business_keys AS (
  SELECT
    0 AS source,
    class_bk,
    hash_key__class_bk,
    _sqlmesh__record_source,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__classes
  UNION ALL
  SELECT
    1 AS primary_source,
    class_id AS class_bk,
    hash_key__class_id AS hash_key__class_bk,
    _sqlmesh__record_source,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__classes
  UNION ALL
  SELECT
    1 AS primary_source,
    class_bk,
    hash_key__class_bk,
    _sqlmesh__record_source,
    _sqlmesh__loaded_at
  FROM silver.staging.dv_stg__hearthstone__cards
), deduplicated AS (
  SELECT
    *
    EXCLUDE (source)
  FROM business_keys
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY class_bk ORDER BY source, _sqlmesh__loaded_at) = 1
)
SELECT
  *
FROM deduplicated