/* Staging model for the Hearthstone spell schools */
MODEL (
  name silver.staging.stg__hearthstone__spell_schools,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__spell_schools
), snapshot_version AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    _sqlmesh__valid_to IS NULL AS _sqlmesh__is_current_record
  FROM source
), casted AS (
  SELECT
    id::INT AS spell_school_id,
    slug::TEXT AS slug,
    name::TEXT AS name,
    gameModes::INT[] AS game_modes,
    _sqlmesh__extracted_at::TIMESTAMP,
    _sqlmesh__hash_diff::BLOB,
    _sqlmesh__loaded_at::TIMESTAMP,
    _sqlmesh__valid_from::TIMESTAMP,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59')::TIMESTAMP AS _sqlmesh__valid_to,
    _sqlmesh__version::INT,
    _sqlmesh__is_current_record::BOOLEAN
  FROM snapshot_version
), final AS (
  SELECT
    *,
    @generate_surrogate_key__sha_256(spell_school_id, _sqlmesh__valid_from) AS spell_school_pit_hk
  FROM casted
)
SELECT
  *
FROM final
WHERE
  _sqlmesh__loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts