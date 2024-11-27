/* Data vault staging model for the Hearthstone cards */
MODEL (
  name silver.staging.dv_stg__hearthstone__cards,
  kind FULL
);

WITH source AS (
  SELECT
    *,
    (@execution_ts||'+00')::TIMESTAMPTZ AS _sqlmesh__loaded_at
  FROM bronze.raw.raw__hearthstone__cards
), keys AS (
  SELECT
    *,
    slug::TEXT AS card_bk,
    parentId::TEXT AS parent_card_bk,
    cardSetId::TEXT AS card_set_bk,
    cardTypeId::TEXT AS card_type_bk,
    classId::TEXT AS class_bk,
    minionTypeId::TEXT AS minion_type_bk,
    rarityId::TEXT AS rarity_bk,
    spellSchoolId::TEXT AS spell_school_bk
  FROM source
), ghost_record AS (
    SELECT
        keys.*
        REPLACE(
            ghost.card_bk AS card_bk,
            ghost._sqlmesh__record_source AS _sqlmesh__record_source,
            ghost._sqlmesh__loaded_at AS _sqlmesh__loaded_at
        )
    
    FROM
        (
            SELECT
                'GHOST' AS card_bk,
                'GHOST_RECORD' AS _sqlmesh__record_source,
                '-infinity'::TIMESTAMPTZ AS _sqlmesh__loaded_at
        ) AS ghost
        
        LEFT JOIN keys
            ON ghost.card_bk = keys.card_bk
            AND ghost._sqlmesh__record_source = keys._sqlmesh__record_source
            AND ghost._sqlmesh__loaded_at = keys._sqlmesh__loaded_at
    
    UNION ALL
    
    SELECT * FROM keys
), hashes AS (
  SELECT
    *,
    @generate_surrogate_key(card_bk, hash_function := 'SHA256') AS hash_key__card_bk,
    @generate_surrogate_key(
      card_bk,
      id,
      armor,
      artistName,
      attack,
      bannedFromSideboard,
      collectible,
      cropImage,
      durability,
      flavorText,
      health,
      image,
      imageGold,
      isZilliaxCosmeticModule,
      isZilliaxFunctionalModule,
      manaCost,
      maxSideboardCards,
      name,
      runeCost,
      slug,
      text,
      hash_function := 'SHA256'
    ) AS hash_diff__card,
    @generate_surrogate_key(parent_card_bk, hash_function := 'SHA256') AS hash_key__parent_card_bk,
    @generate_surrogate_key(card_set_bk, hash_function := 'SHA256') AS hash_key__card_set_bk,
    @generate_surrogate_key(card_type_bk, hash_function := 'SHA256') AS hash_key__card_type_bk,
    @generate_surrogate_key(class_bk, hash_function := 'SHA256') AS hash_key__class_bk,
    @generate_surrogate_key(minion_type_bk, hash_function := 'SHA256') AS hash_key__minion_type_bk,
    @generate_surrogate_key(rarity_bk, hash_function := 'SHA256') AS hash_key__rarity_bk,
    @generate_surrogate_key(spell_school_bk, hash_function := 'SHA256') AS hash_key__spell_school_bk,
    @generate_surrogate_key(card_bk, parent_card_bk, hash_function := 'SHA256') AS hash_key__card_bk__parent_card_bk,
    @generate_surrogate_key(card_bk, card_set_bk, hash_function := 'SHA256') AS hash_key__card_bk__card_set_bk,
    @generate_surrogate_key(card_bk, card_type_bk, hash_function := 'SHA256') AS hash_key__card_bk__card_type_bk,
    @generate_surrogate_key(card_bk, class_bk, hash_function := 'SHA256') AS hash_key__card_bk__class_bk,
    @generate_surrogate_key(card_bk, minion_type_bk, hash_function := 'SHA256') AS hash_key__card_bk__minion_type_bk,
    @generate_surrogate_key(card_bk, rarity_bk, hash_function := 'SHA256') AS hash_key__card_bk__rarity_bk,
    @generate_surrogate_key(card_bk, spell_school_bk, hash_function := 'SHA256') AS hash_key__card_bk__spell_school_bk
  FROM ghost_record
)
SELECT
  *
FROM hashes