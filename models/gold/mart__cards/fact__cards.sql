/* Fact of all the cards in Hearthstone */
MODEL (
  name gold.mart__cards.fact__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (fact__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH fact AS (
  SELECT
    card_id, /* Unique identifier for the card */
    card_set_id, /* Unique identifier for the card set */
    card_type_id, /* Unique identifier for the card type */
    class_id, /* Unique identifier for the class */
    keyword_ids, /* List of unique identifiers for the keywords */
    minion_type_id, /* Unique identifier for the minion type */
    multi_class_ids, /* List of unique identifiers for the multi-class */
    multi_type_ids, /* List of unique identifiers for the multi-type */
    rarity_id, /* Unique identifier for the rarity */
    spell_school_id, /* Unique identifier for the spell school */
    tourist_class_id, /* Unique identifier for the tourist class */
    card_relations, /* List of card relations */
    is_zilliax_cosmetic_module, /* Flag for Zilliax cosmetic module */
    is_zilliax_functional_module, /* Flag for Zilliax functional module */
    mana_cost, /* Mana cost of the card */
    rune_cost['blood'] AS blood_rune_cost, /* Blood rune cost of the card */
    rune_cost['frost'] AS frost_rune_cost, /* Frost rune cost of the card */
    rune_cost['unholy'] AS unholy_rune_cost, /* Unholy rune cost of the card */
    blood_rune_cost + frost_rune_cost + unholy_rune_cost AS total_rune_cost, /* Total rune cost of the card */
    _dlt_extracted_at AS fact__extracted_at, /* Timestamp when the record was extracted */
    _sqlmesh_loaded_at AS fact__loaded_at, /* Timestamp when the record was loaded */
    _sqlmesh_version AS fact__version, /* Version of the record */
    _sqlmesh_valid_from AS fact__valid_from, /* Timestamp when the record is valid from */
    _sqlmesh_valid_to AS fact__valid_to, /* Timestamp when the record is valid to */
    _sqlmesh_is_current_record AS fact__is_current_record /* Flag for the current record */
  FROM silver.staging.stg__hearthstone__cards
), dimensions AS (
  SELECT
    dim__cardbacks.cardback_pit_hk, /* Unique identifier in time for the cardback */
    dim__cards.card_pit_hk, /* Unique identifier in time for the card */
    dim__classes.class_pit_hk, /* Unique identifier in time for the class */
    dim__minion_types.minion_type_pit_hk, /* Unique identifier in time for the minion type */
    dim__rarities.rarity_pit_hk, /* Unique identifier in time for the rarity */
    dim__sets.set_pit_hk, /* Unique identifier in time for the set */
    dim__spell_schools.spell_school_pit_hk, /* Unique identifier in time for the spell school */
    dim__tourist_classes.tourist_class_pit_hk, /* Unique identifier in time for the tourist class */
    dim__types.type_pit_hk, /* Unique identifier in time for the type */
    fact.*
  FROM fact
  LEFT JOIN gold.mart__cards.dim__cardbacks
    ON fact.card_id = dim__cardbacks.card_id
    AND fact.fact__valid_from BETWEEN dim__cardbacks.cardback__valid_from AND dim__cardbacks.cardback__valid_to
  LEFT JOIN gold.mart__cards.dim__cards
    ON fact.card_id = dim__cards.card_id
    AND fact.fact__valid_from BETWEEN dim__cards.card__valid_from AND dim__cards.card__valid_to
  LEFT JOIN gold.mart__cards.dim__classes
    ON fact.class_id = dim__classes.class_id
    AND fact.fact__valid_from BETWEEN dim__classes.class__valid_from AND dim__classes.class__valid_to
  LEFT JOIN gold.mart__cards.dim__tourist_classes
    ON fact.tourist_class_id = dim__tourist_classes.tourist_class_id
    AND fact.fact__valid_from BETWEEN dim__tourist_classes.tourist_class__valid_from AND dim__tourist_classes.tourist_class__valid_to
  LEFT JOIN gold.mart__cards.dim__minion_types
    ON fact.minion_type_id = dim__minion_types.minion_type_id
    AND fact.fact__valid_from BETWEEN dim__minion_types.minion_type__valid_from AND dim__minion_types.minion_type__valid_to
  LEFT JOIN gold.mart__cards.dim__rarities
    ON fact.rarity_id = dim__rarities.rarity_id
    AND fact.fact__valid_from BETWEEN dim__rarities.rarity__valid_from AND dim__rarities.rarity__valid_to
  LEFT JOIN gold.mart__cards.dim__sets
    ON fact.card_set_id = dim__sets.set_id
    AND fact.fact__valid_from BETWEEN dim__sets.set__valid_from AND dim__sets.set__valid_to
  LEFT JOIN gold.mart__cards.dim__spell_schools
    ON fact.spell_school_id = dim__spell_schools.spell_school_id
    AND fact.fact__valid_from BETWEEN dim__spell_schools.spell_school__valid_from AND dim__spell_schools.spell_school__valid_to
  LEFT JOIN gold.mart__cards.dim__types
    ON fact.card_type_id = dim__types.type_id
    AND fact.fact__valid_from BETWEEN dim__types.type__valid_from AND dim__types.type__valid_to
), final AS (
  SELECT
    'cards' AS fact_name, /* Name of the fact table */
    @generate_surrogate_key__sha_256(
      fact_name,
      cardback_pit_hk,
      card_pit_hk,
      class_pit_hk,
      minion_type_pit_hk,
      rarity_pit_hk,
      set_pit_hk,
      tourist_class_pit_hk,
      type_pit_hk,
      card_relations,
      keyword_ids,
      multi_class_ids,
      multi_type_ids,
      spell_school_id
    ) AS fact_record_hk, /* Unique identifier for the fact table */
    *
  FROM dimensions
)
SELECT
  *
FROM final
WHERE
  fact__loaded_at BETWEEN @start_ts AND @end_ts;

@export_to_parquet(@this_model, "data")