/* Data vault staging model for the Hearthstone cards */
MODEL (
  name silver.staging.dv_stg__hearthstone__cards,
  kind FULL
);

WITH source AS (
  SELECT
    *,
    'raw__hearthstone__cards' AS _sqlmesh_record_source,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_extracted_at,
    @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
  FROM bronze.raw.raw__hearthstone__cards
), keys AS (
  SELECT
    *,
    id::TEXT AS card_id,
    slug::TEXT AS card_bk,
    parent_id::TEXT AS parent_card_bk,
    card_set_id::TEXT AS card_set_bk,
    card_type_id::TEXT AS card_type_bk,
    class_id::TEXT AS class_bk,
    minion_type_id::TEXT AS minion_type_bk,
    rarity_id::TEXT AS rarity_bk,
    spell_school_id::TEXT AS spell_school_bk
  FROM source
), ghost_record AS (
  SELECT
    keys.*
    REPLACE (ghost._sqlmesh_record_source AS _sqlmesh_record_source, ghost._sqlmesh_loaded_at AS _sqlmesh_loaded_at)
  FROM (
    SELECT
      'GHOST_RECORD' AS _sqlmesh_record_source,
      '0001-01-01 00:00:00'::TIMESTAMP AS _dlt_extracted_at,
      '0001-01-01 00:00:00'::TIMESTAMP AS _sqlmesh_loaded_at
  ) AS ghost
  LEFT JOIN keys
    ON ghost._sqlmesh_record_source = keys._sqlmesh_record_source
    AND ghost._dlt_extracted_at = keys._dlt_extracted_at
    AND ghost._sqlmesh_loaded_at = keys._sqlmesh_loaded_at
  UNION ALL
  SELECT
    *
  FROM keys
), hashes AS (
  SELECT
    *,
    @generate_surrogate_key(card_id, hash_function := 'SHA256') AS hash_key__card_id,
    @generate_surrogate_key(card_bk, hash_function := 'SHA256') AS hash_key__card_bk,
    @generate_surrogate_key(
      card_bk,
      id,
      armor,
      artist_name,
      attack,
      banned_from_sideboard,
      collectible,
      crop_image,
      durability,
      flavor_text,
      health,
      image,
      image_gold,
      is_zilliax_cosmetic_module,
      is_zilliax_functional_module,
      mana_cost,
      max_sideboard_cards,
      name,
      rune_cost,
      slug,
      text,
      hash_function := 'SHA256'
    ) AS _sqlmesh_hash_diff,
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