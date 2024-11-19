/* Data vault staging model for the Hearthstone cards */
MODEL (
  name silver.data_vault.dv_stg__hearthstone__cards,
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
    name::TEXT AS name,
    REPLACE(runeCost, '''', '"')::JSON::STRUCT(blood INT, frost INT, unholy INT) AS rune_cost,
    slug::TEXT AS slug,
    text::TEXT AS text,
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
    slug AS card_bk
  FROM casted
), ghost_record AS (
  SELECT
    *
  FROM (
    SELECT
      'Ghost Record' AS card_bk,
      '0001-01-01 00:00:00'::TIMESTAMP AS _sqlmesh__valid_from,
      '9999-12-31 23:59:59'::TIMESTAMP AS _sqlmesh__valid_to
  )
  LEFT JOIN business_keys
    USING (card_bk, _sqlmesh__valid_from, _sqlmesh__valid_to)
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
    @generate_surrogate_key__sha_256(card_bk) AS card_hk,
    @generate_surrogate_key__sha_256(card_bk, _sqlmesh__valid_from) AS card_pit_hk
  FROM union_ghost_record
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