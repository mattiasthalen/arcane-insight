/* Fact of all the cards in Hearthstone */
MODEL (
  name gold.mart__cards.fact__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (fact__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__cards
), dim__cards AS (
  SELECT
    *
  FROM gold.common.common_dim__cards
), dim__classes AS (
  SELECT
    *
  FROM gold.common.common_dim__classes
), dim__types AS (
  SELECT
    *
  FROM gold.common.common_dim__types
), dim__minion_types AS (
  SELECT
    *
  FROM gold.common.common_dim__minion_types
), fact AS (
  SELECT
    card_id, /* Unique identifier for the card */
    card_relations_hk, /* Unique identifier for the card relations */
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
    _sqlmesh__extracted_at AS fact__extracted_at, /* Timestamp when the record was extracted */
    _sqlmesh__loaded_at AS fact__loaded_at, /* Timestamp when the record was loaded */
    _sqlmesh__version AS fact__version, /* Version of the record */
    _sqlmesh__valid_from AS fact__valid_from, /* Timestamp when the record is valid from */
    _sqlmesh__valid_to AS fact__valid_to, /* Timestamp when the record is valid to */
    _sqlmesh__is_current_record AS fact__is_current_record /* Flag for the current record */
  FROM source
), dimensions AS (
  SELECT
    dim__cards.card_pit_hk, /* Unique identifier in time for the card */
    dim__classes.class_pit_hk, /* Unique identifier in time for the class */
    dim__types.type_pit_hk, /* Unique identifier in time for the type */
    dim__minion_types.minion_type_pit_hk, /* Unique identifier in time for the minion type */
    fact.*
  FROM fact
  LEFT JOIN dim__cards
    ON fact.card_id = dim__cards.card_id
    AND fact.fact__valid_from BETWEEN dim__cards.card__valid_from AND dim__cards.card__valid_to
    LEFT JOIN dim__classes
      ON fact.class_id = dim__classes.class_id
      AND fact.fact__valid_from BETWEEN dim__classes.class__valid_from AND dim__classes.class__valid_to
    LEFT JOIN dim__types
      ON fact.card_type_id = dim__types.type_id
      AND fact.fact__valid_from BETWEEN dim__types.type__valid_from AND dim__types.type__valid_to
    LEFT JOIN dim__minion_types
      ON fact.minion_type_id = dim__minion_types.minion_type_id
      AND fact.fact__valid_from BETWEEN dim__minion_types.minion_type__valid_from AND dim__minion_types.minion_type__valid_to
), final AS (
  SELECT
    'cards' AS fact_name, /* Name of the fact table */
    @generate_surrogate_key__sha_256(
      fact_name,
      card_relations_hk,
      card_set_id,
      card_type_id,
      card_pit_hk,
      class_pit_hk,
      type_pit_hk,
      minion_type_pit_hk,
      keyword_ids,
      multi_class_ids,
      multi_type_ids,
      rarity_id,
      spell_school_id,
      tourist_class_id
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