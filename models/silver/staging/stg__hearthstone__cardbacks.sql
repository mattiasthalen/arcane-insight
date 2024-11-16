/* Staging model for the Hearthstone cardbacks */
MODEL (
  name silver.staging.stg__hearthstone__cardbacks,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  ),
  allow_partials TRUE
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__cardbacks
), valid_range AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    _sqlmesh__valid_to IS NULL AS _sqlmesh__is_current_record,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59') _sqlmesh__valid_to
  FROM source
), casted AS (
  SELECT
    id::TEXT AS card_id,
    cardbackCategory::TEXT AS cardback_category,
    text::TEXT AS text,
    name::TEXT AS name,
    sortCategory::TEXT AS sort_category,
    slug::TEXT AS slug,
    image::TEXT AS image_url,
    _sqlmesh__extracted_at::TIMESTAMP,
    _sqlmesh__hash_diff::BLOB,
    _sqlmesh__loaded_at::TIMESTAMP,
    _sqlmesh__version::INT,
    _sqlmesh__valid_from::TIMESTAMP,
    _sqlmesh__valid_to::TIMESTAMP,
    _sqlmesh__is_current_record::BOOLEAN
  FROM valid_range
), final AS (
  SELECT
    *,
    @generate_surrogate_key__sha_256(card_id, _sqlmesh__valid_from) AS cardback_pit_hk
  FROM casted
)
SELECT
  *
FROM final
WHERE
  _sqlmesh__loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts