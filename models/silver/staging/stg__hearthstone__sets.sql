/* Staging model for the Hearthstone sets */
MODEL (
  name silver.staging.stg__hearthstone__sets,
  kind FULL
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__sets
), valid_range AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    LAG(_sqlmesh__loaded_at, 1, '1970-01-01 00:00:00') OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__valid_from,
    LEAD(_sqlmesh__loaded_at, 1, '9999-12-31 23:59:59') OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__valid_to,
    _sqlmesh__valid_to = '9999-12-31 23:59:59' AS _sqlmesh__is_current_record
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
    _sqlmesh__version::INT,
    _sqlmesh__valid_from::TIMESTAMP,
    _sqlmesh__valid_to::TIMESTAMP,
    _sqlmesh__is_current_record::BOOLEAN
  FROM valid_range
), final AS (
  SELECT
    *,
    @generate_surrogate_key__sha_256(set_id, _sqlmesh__valid_from) AS set_pit_hk
  FROM casted
)
SELECT
  *
FROM final /* WHERE */ /*     _sqlmesh__loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts */