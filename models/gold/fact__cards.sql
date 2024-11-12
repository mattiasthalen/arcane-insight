MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.stg__hearthstone__cards
), final AS (
  SELECT
    @generate_surrogate_key__sha_256(
      'cards',
      card_id,
      card_set_id,
      card_type_id,
      child_card_ids,
      class_id,
      copy_of_card_id,
      keyword_ids,
      minion_type_id,
      multi_class_ids,
      multi_type_ids,
      parent_card_id,
      rarity_id,
      spell_school_id,
      tourist_class_id
    ) AS fact_record_id,
    card_id AS fact_card_id,
    child_card_ids,
    copy_of_card_id,
    parent_card_id,
    card_set_id,
    card_type_id,
    class_id,
    keyword_ids,
    minion_type_id,
    multi_class_ids,
    multi_type_ids,
    rarity_id,
    spell_school_id,
    tourist_class_id,
    is_zilliax_cosmetic_module,
    is_zilliax_functional_module,
    mana_cost,
    rune_cost['blood'] AS blood_rune_cost,
    rune_cost['frost'] AS frost_rune_cost,
    rune_cost['unholy'] AS unholy_rune_cost,
    blood_rune_cost + frost_rune_cost + unholy_rune_cost AS total_rune_cost,
    _sqlmesh__extracted_at,
    _sqlmesh__loaded_at,
    _sqlmesh__record_version,
    _sqlmesh__record_valid_from,
    _sqlmesh__record_valid_to,
    _sqlmesh__is_current_record
  FROM source
)
SELECT
  *
FROM final
WHERE
  _sqlmesh__loaded_at BETWEEN @start_ts AND @end_ts