/* Bag model for the Hearthstone cards */
MODEL (
  name silver.hook.bag__hearthstone__cards,
  kind VIEW
);

WITH source AS (
  SELECT
    *,
    'battle_net' AS _sqlmesh_record_source
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
    _sqlmesh_is_current_record::BOOLEAN,
    _sqlmesh_record_source::TEXT
  FROM snapshot_version
), hook_keys AS (
  SELECT
    @generate_hook_key(1, card_id) AS hook__card__id,
    @generate_hook_key(1, copy_of_card_id) AS hook__card__id__copy,
    @generate_hook_key(1, parent_card_id) AS hook__card__id__parent,
    @generate_hook_key(2, card_set_id) AS hook__card_set__id,
    @generate_hook_key(3, card_type_id) AS hook__card_type__id,
    @generate_hook_key(4, class_id) AS hook__class__id,
    @generate_hook_key(4, tourist_class_id) AS hook__class__id__tourist,
    @generate_hook_key(5, minion_type_id) AS hook__minion_type__id,
    @generate_hook_key(6, spell_school_id) AS hook__spell_school__id,
    *
  FROM casted
)
SELECT
  *
FROM hook_keys