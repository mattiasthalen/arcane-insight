/* Data vault staging model for the Hearthstone classes */
MODEL (
    enabled FALSE,
  name silver.data_vault.dv_stg__hearthstone__classes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__classes
), snapshot_version AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    _sqlmesh__valid_to IS NULL AS _sqlmesh__is_current_record
  FROM source
), casted AS (
  SELECT
    slug::TEXT AS slug,
    id::INT AS class_id,
    name::TEXT AS name,
    cardId::INT AS card_id,
    heroPowerCardId::INT AS hero_power_card_id,
    alternateHeroCardIds::INT[] AS alternate_hero_card_ids,
    _sqlmesh__extracted_at::TIMESTAMP,
    _sqlmesh__hash_diff::BLOB,
    _sqlmesh__loaded_at::TIMESTAMP,
    _sqlmesh__valid_from::TIMESTAMP,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59')::TIMESTAMP AS _sqlmesh__valid_to,
    _sqlmesh__version::INT,
    _sqlmesh__is_current_record::BOOLEAN
  FROM snapshot_version
), business_keys AS (
  SELECT
    *,
    slug AS class_bk
  FROM casted
), ghost_record AS (
  SELECT
    *
  FROM (
    SELECT
      'Ghost Record' AS class_bk,
      '0001-01-01 00:00:00'::TIMESTAMP AS _sqlmesh__valid_from,
      '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__valid_to
  )
  LEFT JOIN business_keys
    USING (class_bk, _sqlmesh__valid_from, _sqlmesh__valid_to)
  LIMIT 1
), union_ghost_record AS (
  SELECT
    *
  FROM business_keys
  UNION ALL
  SELECT
    *
  FROM ghost_record
), hash_keys AS (
  SELECT
    *,
    @generate_surrogate_key__sha_256(class_bk) AS class_hk,
    @generate_surrogate_key__sha_256(class_bk, _sqlmesh__valid_from) AS class_pit_hk
  FROM union_ghost_record
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
  _sqlmesh__loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts