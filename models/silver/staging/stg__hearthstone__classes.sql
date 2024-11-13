/* Staging model for the Hearthstone classes */
MODEL (
  name silver.staging.stg__hearthstone__classes,
  kind FULL
  -- kind INCREMENTAL_BY_TIME_RANGE (
  --   time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  -- )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__classes
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
    slug::TEXT AS class_slug,
    id::INT AS class_id,
    name::TEXT AS class_name,
    cardId::INT AS card_id,
    heroPowerCardId::INT AS hero_power_card_id,
    alternateHeroCardIds::INT[] AS alternate_hero_card_ids,
    _sqlmesh__extracted_at::TIMESTAMP,
    _sqlmesh__hash_diff::BLOB,
    _sqlmesh__loaded_at::TIMESTAMP,
    _sqlmesh__version::INT,
    _sqlmesh__valid_from::TIMESTAMP,
    _sqlmesh__valid_to::TIMESTAMP,
    _sqlmesh__is_current_record::BOOLEAN
  FROM valid_range
), hash_keys AS (
  SELECT
    *,
    @generate_surrogate_key__sha_256(class_id, _sqlmesh__valid_from) AS class_pit_hk
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
-- WHERE
  -- _sqlmesh__loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts