/* Staging model for the Hearthstone cardbacks */
MODEL (
  name silver.staging.stg__hearthstone__cardbacks,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh_loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__cardbacks
), snapshot_version AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
    _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record
  FROM source
), casted AS (
  SELECT
    id::TEXT AS card_id,
    text /* cardback_category::TEXT AS cardback_category, */::TEXT AS text,
    name::TEXT AS name,
    sort_category::TEXT AS sort_category,
    slug::TEXT AS slug,
    image::TEXT AS image_url,
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
    @generate_surrogate_key__sha_256(card_id, _sqlmesh_valid_from) AS cardback_pit_hk
  FROM casted
)
SELECT
  *
FROM final
WHERE
  _sqlmesh_loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts