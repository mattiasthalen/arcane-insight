/* Staging model for the Hearthstone cards */
MODEL (
  name silver.staging.stg__hearthstone__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snapshot.snp__hearthstone__cards
), snapshot_version AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__version,
    _sqlmesh__valid_to IS NULL AS _sqlmesh__is_current_record
  FROM source
), casted AS (
  SELECT
    id::INT AS card_id,
    cardSetId::INT AS card_set_id,
    cardTypeId::INT AS card_type_id,
    childIds::INT[] AS child_card_ids,
    classId::INT AS class_id,
    copyOfCardId::INT AS copy_of_card_id,
    keywordIds::INT[] AS keyword_ids,
    minionTypeId::INT AS minion_type_id,
    multiClassIds::INT[] AS multi_class_ids,
    multiTypeIds::INT[] AS multi_type_ids,
    parentId::INT AS parent_card_id,
    rarityId::INT AS rarity_id,
    spellSchoolId::INT AS spell_school_id,
    touristClassId::INT AS tourist_class_id,
    armor::INT AS armor,
    artistName::TEXT AS artist_name,
    attack::INT AS attack,
    COALESCE(bannedFromSideboard::INT, 0)::BOOLEAN AS banned_from_sideboard,
    collectible::BOOLEAN AS collectible,
    cropImage::TEXT AS crop_image,
    durability::INT AS durability,
    flavorText::TEXT AS flavor_text,
    health::INT AS health,
    image::TEXT AS image,
    imageGold::TEXT AS image_gold,
    isZilliaxCosmeticModule::BOOLEAN AS is_zilliax_cosmetic_module,
    isZilliaxFunctionalModule::BOOLEAN AS is_zilliax_functional_module,
    manaCost::INT AS mana_cost,
    maxSideboardCards::INT AS max_sideboard_cards,
    name::TEXT AS card_name,
    REPLACE(runeCost, '''', '"')::JSON::STRUCT(blood INT, frost INT, unholy INT) AS rune_cost,
    slug::TEXT AS card_slug,
    text::TEXT AS card_text,
    _sqlmesh__extracted_at::TIMESTAMP,
    _sqlmesh__hash_diff::BLOB,
    _sqlmesh__loaded_at::TIMESTAMP,
    _sqlmesh__valid_from::TIMESTAMP,
    COALESCE(_sqlmesh__valid_to, '9999-12-31 23:59:59')::TIMESTAMP AS _sqlmesh__valid_to,
    _sqlmesh__version::INT,
    _sqlmesh__is_current_record::BOOLEAN
  FROM snapshot_version
), hash_keys AS (
  SELECT
    *,
    @generate_surrogate_key__sha_256(card_id, _sqlmesh__valid_from) AS card_pit_hk
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
  _sqlmesh__loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts