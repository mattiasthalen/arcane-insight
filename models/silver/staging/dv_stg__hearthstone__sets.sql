/* Data vault staging model for the Hearthstone sets */
MODEL (
  name silver.staging.dv_stg__hearthstone__sets,
  kind FULL
);

WITH source AS (
  SELECT
    *,
    @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
  FROM bronze.raw.raw__hearthstone__sets
), keys AS (
  SELECT
    *,
    id::TEXT AS card_set_id,
    slug::TEXT AS card_set_bk
  FROM source
), ghost_record AS (
  SELECT
    keys.*
    REPLACE (ghost._sqlmesh__record_source AS _sqlmesh__record_source, ghost._sqlmesh__loaded_at AS _sqlmesh__loaded_at)
  FROM (
    SELECT
      'GHOST_RECORD' AS _sqlmesh__record_source,
      '0001-01-01 00:00:00'::TIMESTAMP AS _sqlmesh__extracted_at,
      '0001-01-01 00:00:00'::TIMESTAMP AS _sqlmesh__loaded_at
  ) AS ghost
  LEFT JOIN keys
    ON ghost._sqlmesh__record_source = keys._sqlmesh__record_source
    AND ghost._sqlmesh__extracted_at = keys._sqlmesh__extracted_at
    AND ghost._sqlmesh__loaded_at = keys._sqlmesh__loaded_at
  UNION ALL
  SELECT
    *
  FROM keys
), hashes AS (
  SELECT
    *,
    @generate_surrogate_key(card_set_id, hash_function := 'SHA256') AS hash_key__card_set_id,
    @generate_surrogate_key(card_set_bk, hash_function := 'SHA256') AS hash_key__card_set_bk,
    @generate_surrogate_key(
      card_set_bk,
      id,
      name,
      slug,
      hyped,
      type,
      collectibleCount,
      collectibleRevealedCount,
      nonCollectibleCount,
      nonCollectibleRevealedCount,
      hash_function := 'SHA256'
    ) AS _sqlmesh__hash_diff
  FROM ghost_record
)
SELECT
  *
FROM hashes