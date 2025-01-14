/* Data vault staging model for the Hearthstone spell schools */
MODEL (
  name silver.staging.dv_stg__hearthstone__spell_schools,
  kind FULL
);

WITH source AS (
  SELECT
    *,
    'raw__hearthstone__spell_schools' AS _sqlmesh_record_source,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
    @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
  FROM bronze.raw.raw__hearthstone__spell_schools
), keys AS (
  SELECT
    *,
    id::TEXT AS spell_school_id,
    slug::TEXT AS spell_school_bk
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
    @generate_surrogate_key(spell_school_id, hash_function := 'SHA256') AS hash_key__spell_school_id,
    @generate_surrogate_key(spell_school_bk, hash_function := 'SHA256') AS hash_key__spell_school_bk,
    @generate_surrogate_key(spell_school_bk, slug, id, name, game_modes, hash_function := 'SHA256') AS _sqlmesh_hash_diff
  FROM ghost_record
)
SELECT
  *
FROM hashes