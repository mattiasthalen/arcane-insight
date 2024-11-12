MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (fact__record_valid_from, '%Y-%m-%d %H:%M:%S')
  )
);

SELECT
  @generate_surrogate_key__sha_256(
    'cards',
    link__cards.link_pit_hk,
    card_set_id,
    card_type_id,
    class_id,
    keyword_ids,
    minion_type_id,
    multi_class_ids,
    multi_type_ids,
    rarity_id,
    spell_school_id,
    tourist_class_id
  ) AS fact_record_id,
  link__cards.link_pit_hk,
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
  _sqlmesh__extracted_at AS fact__extracted_at,
  _sqlmesh__loaded_at AS fact__loaded_at,
  _sqlmesh__record_version AS fact__record_version,
  _sqlmesh__record_valid_from AS fact__record_valid_from,
  _sqlmesh__record_valid_to AS fact__record_valid_to,
  _sqlmesh__is_current_record AS fact__is_current_record
FROM silver.stg__hearthstone__cards
LEFT JOIN gold.link__cards
  ON stg__hearthstone__cards.card_relations = link__cards.card_relations
  AND stg__hearthstone__cards._sqlmesh__record_valid_from BETWEEN link__cards.link__record_valid_from AND link__cards.link__record_valid_to
WHERE
  fact__record_valid_from BETWEEN @start_ts AND @end_ts