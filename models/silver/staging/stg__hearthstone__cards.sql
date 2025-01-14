/* Staging model for the Hearthstone cards */
MODEL (
  name silver.staging.stg__hearthstone__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh_loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__cards
), snapshot_version AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh_loaded_at) AS _sqlmesh_version,
    _sqlmesh_valid_to IS NULL AS _sqlmesh_is_current_record
  FROM source
), casted AS (
  SELECT
    id::INT AS card_id,
    card_set_id::INT AS card_set_id,
    card_type_id::INT AS card_type_id,
    child_ids::INT[] AS child_card_ids,
    class_id::INT AS class_id,
    copy_of_card_id::INT AS copy_of_card_id,
    keyword_ids::INT[] AS keyword_ids,
    minion_type_id::INT AS minion_type_id,
    multi_class_ids::INT[] AS multi_class_ids,
    multi_type_ids::INT[] AS multi_type_ids,
    parent_id::INT AS parent_card_id,
    rarity_id::INT AS rarity_id,
    spell_school_id::INT AS spell_school_id,
    tourist_class_id::INT AS tourist_class_id,
    armor::INT AS armor,
    artist_name::TEXT AS artist_name,
    attack::INT AS attack,
    COALESCE(banned_from_sideboard::INT, 0)::BOOLEAN AS banned_from_sideboard,
    collectible::BOOLEAN AS collectible,
    crop_image::TEXT AS crop_image,
    durability::INT AS durability,
    flavor_text::TEXT AS flavor_text,
    health::INT AS health,
    image::TEXT AS image,
    image_gold::TEXT AS image_gold,
    is_zilliax_cosmetic_module::BOOLEAN AS is_zilliax_cosmetic_module,
    is_zilliax_functional_module::BOOLEAN AS is_zilliax_functional_module,
    mana_cost::INT AS mana_cost,
    max_sideboard_cards::INT AS max_sideboard_cards,
    name::TEXT AS card_name,
    REPLACE(rune_cost, '''', '"')::JSON::STRUCT(blood INT, frost INT, unholy INT) AS rune_cost,
    slug::TEXT AS card_slug,
    text::TEXT AS card_text,
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
    @generate_surrogate_key__sha_256(card_id, _sqlmesh_valid_from) AS card_pit_hk
  FROM casted
), final AS (
  SELECT
    *,
    {'card_id': card_id, 'parent_card_id': parent_card_id, 'copy_of_card_id': copy_of_card_id, 'child_card_ids': child_card_ids} AS card_relations,
    @generate_surrogate_key__sha_256(card_relations) AS card_relations_hk
  FROM hash_keys
)
SELECT
  *
FROM final
WHERE
  _sqlmesh_loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts