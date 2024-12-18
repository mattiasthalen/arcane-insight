/* Staging model for the Hearthstone sets */
MODEL (
  name silver.staging.stg__hearthstone__sets,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__sets
), snapshot_version AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    _sqlmesh__valid_to IS NULL AS _sqlmesh__is_current_record
  FROM source
), casted AS (
  SELECT
    id::INT AS set_id,
    name::TEXT AS name,
    slug::TEXT AS slug,
    hyped::BOOLEAN AS hyped,
    type::TEXT AS type,
    collectibleCount::INT AS collectible_count,
    collectibleRevealedCount::INT AS collectible_revealed_count,
    nonCollectibleCount::INT AS non_collectible_count,
    nonCollectibleRevealedCount::INT AS non_collectible_revealed_count,
    aliasSetIds::INT[] AS alias_set_ids,
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
    @generate_surrogate_key__sha_256(set_id, _sqlmesh__valid_from) AS set_pit_hk
  FROM casted
)
SELECT
  *
FROM final
WHERE
  _sqlmesh__loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts