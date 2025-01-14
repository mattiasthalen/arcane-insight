/* Staging model for the Hearthstone classes */
MODEL (
  name silver.staging.stg__hearthstone__classes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh_loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__classes
), snapshot_version AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
    _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record
  FROM source
), casted AS (
  SELECT
    slug::TEXT AS class_slug,
    id::INT AS class_id,
    name::TEXT AS class_name,
    card_id::INT AS card_id,
    hero_power_card_id::INT AS hero_power_card_id,
    alternate_hero_card_ids::INT[] AS alternate_hero_card_ids,
    _dlt_extracted_at::TIMESTAMP,
    _sqlmesh_hash_diff::BLOB,
    _sqlmesh_loaded_at::TIMESTAMP,
    _sqlmesh_valid_from::TIMESTAMP,
    COALESCE(_sqlmesh_valid_to, '9999-12-31 23:59:59')::TIMESTAMP AS _sqlmesh_valid_to,
    _sqlmesh_version::INT,
    _sqlmesh_is_current_record::BOOLEAN
  FROM snapshot_version
), hash_keys AS (
  SELECT
    *,
    @generate_surrogate_key__sha_256(class_id, _sqlmesh_valid_from) AS class_pit_hk
  FROM casted
), final AS (
  SELECT
    *,
    {'card_id': card_id, 'hero_power_card_id': hero_power_card_id, 'alternate_hero_card_ids': alternate_hero_card_ids} AS card_relations,
    @generate_surrogate_key__sha_256(card_relations) AS card_relations_hk
  FROM hash_keys
)
SELECT
  *
FROM final
WHERE
  _sqlmesh_loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts