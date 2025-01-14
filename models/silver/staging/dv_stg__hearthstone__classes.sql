/* Data vault staging model for the Hearthstone classes */
MODEL (
  name silver.staging.dv_stg__hearthstone__classes,
  kind FULL
);

WITH source AS (
  SELECT
    *,
    'raw__hearthstone__classes' AS _sqlmesh_record_source,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
    @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
  FROM bronze.raw.raw__hearthstone__classes
), keys AS (
  SELECT
    *,
    id::TEXT AS class_id,
    slug::TEXT AS class_bk,
    card_id::TEXT AS card_bk,
    hero_power_card_id::TEXT AS hero_power_card_bk
  FROM source
), ghost_record AS (
  SELECT
    keys.*
    REPLACE (ghost._sqlmesh_record_source AS _sqlmesh_record_source, ghost._sqlmesh_loaded_at AS _sqlmesh_loaded_at)
  FROM (
    SELECT
      'GHOST_RECORD' AS _sqlmesh_record_source,
      '0001-01-01 00:00:00'::TIMESTAMP AS _dlt_extracted_at,
      '0001-01-01 00:00:00'::TIMESTAMP AS _sqlmesh_loaded_at
  ) AS ghost
  LEFT JOIN keys
    ON ghost._sqlmesh_record_source = keys._sqlmesh_record_source
    AND ghost._dlt_extracted_at = keys._dlt_extracted_at
    AND ghost._sqlmesh_loaded_at = keys._sqlmesh_loaded_at
  UNION ALL
  SELECT
    *
  FROM keys
), hashes AS (
  SELECT
    *,
    @generate_surrogate_key(class_id, hash_function := 'SHA256') AS hash_key__class_id,
    @generate_surrogate_key(class_bk, hash_function := 'SHA256') AS hash_key__class_bk,
    @generate_surrogate_key(class_bk, slug, id, name, hash_function := 'SHA256') AS _sqlmesh_hash_diff,
    @generate_surrogate_key(card_bk, hash_function := 'SHA256') AS hash_key__card_bk,
    @generate_surrogate_key(hero_power_card_bk, hash_function := 'SHA256') AS hash_key__hero_power_card_bk,
    @generate_surrogate_key(slug, id, name, hash_function := 'SHA256') AS hash_diff__class,
    @generate_surrogate_key(class_bk, card_bk, hash_function := 'SHA256') AS hash_key__class_bk__card_bk,
    @generate_surrogate_key(class_bk, hero_power_card_bk, hash_function := 'SHA256') AS hash_key__class_bk__hero_power_card_bk
  FROM ghost_record
)
SELECT
  *
FROM hashes