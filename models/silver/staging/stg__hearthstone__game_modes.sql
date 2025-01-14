/* Staging model for the Hearthstone game modes */
MODEL (
  name silver.staging.stg__hearthstone__game_modes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh_loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__game_modes
), snapshot_version AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
    _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record
  FROM source
), casted AS (
  SELECT
    id::INT AS game_mode_id,
    slug::TEXT AS slug,
    name::TEXT AS name,
    _dlt_extracted_at::TIMESTAMP,
    _sqlmesh_hash_diff::BLOB,
    _sqlmesh_loaded_at::TIMESTAMP,
    _sqlmesh_valid_from::TIMESTAMP,
    COALESCE(_sqlmesh_valid_to, '9999-12-31 23:59:59')::TIMESTAMP AS _sqlmesh_valid_to,
    _sqlmesh_version::INT,
    _sqlmesh_is_current_record::BOOLEAN
  FROM snapshot_version
), final AS (
  SELECT
    *,
    @generate_surrogate_key__sha_256(game_mode_id, _sqlmesh_valid_from) AS game_mode_pit_hk
  FROM casted
)
SELECT
  *
FROM final
WHERE
  _sqlmesh_loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts