MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM bronze.snp__hearthstone__cards
), valid_range AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__record_version,
    LAG(_sqlmesh__loaded_at, 1, '1970-01-01 00:00:00') OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__record_valid_from,
    LEAD(_sqlmesh__loaded_at, 1, '9999-12-31 23:59:59') OVER (PARTITION BY id ORDER BY _sqlmesh__loaded_at) AS _sqlmesh__record_valid_to,
    _sqlmesh__record_valid_to = '9999-12-31 23:59:59' AS _sqlmesh__is_current_record
  FROM source
), casted AS (
  SELECT
    id::INT AS card_id,
    cardSetId::INT AS card_set_d,
    cardTypeId::INT AS card_type_d,
    childIds::INT[] AS child_ids,
    classId::INT AS class_id,
    copyOfCardId::INT AS copy_of_card_id,
    keywordIds::INT[] AS keyword_ids,
    minionTypeId::INT AS minion_type_id,
    multiClassIds::INT[] AS multi_class_ids,
    multiTypeIds::INT[] AS multi_type_ids,
    parentId::INT AS parent_id,
    rarityId::INT AS rarity_id,
    spellSchoolId::INT AS spell_school_id,
    touristClassId::INT AS tourist_class_id,
    armor::TEXT AS armor,
    artistName::TEXT AS artist_name,
    attack::TEXT AS attack,
    bannedFromSideboard::TEXT AS banned_from_sideboard,
    collectible::TEXT AS collectible,
    cropImage::TEXT AS crop_image,
    durability::TEXT AS durability,
    flavorText::TEXT AS flavor_text,
    health::TEXT AS health,
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
    _sqlmesh__record_version::INT,
    _sqlmesh__record_valid_from::TIMESTAMP,
    _sqlmesh__record_valid_to::TIMESTAMP,
    _sqlmesh__is_current_record::BOOLEAN
  FROM valid_range
)
SELECT
  *
FROM casted
WHERE
  _sqlmesh__loaded_at::TIMESTAMP BETWEEN @start_ts AND @end_ts