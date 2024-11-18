/* Mart model of the cards from Hearthstone */
MODEL (
  name gold.mart.mart__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (fact__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

SELECT
  fact__cards.fact_name,
  fact__cards.fact_record_hk,
  dim__cardbacks.cardback_category,
  dim__cardbacks.cardback__text,
  dim__cardbacks.cardback__name,
  dim__cardbacks.cardback__sort_category,
  dim__cardbacks.cardback__slug,
  dim__cardbacks.cardback__image_url,
  dim__cardbacks.cardback__extracted_at,
  dim__cardbacks.cardback__loaded_at,
  dim__cardbacks.cardback__hash_diff,
  dim__cardbacks.cardback__version,
  dim__cardbacks.cardback__valid_from,
  dim__cardbacks.cardback__valid_to,
  dim__cardbacks.cardback__is_current_record,
  dim__cards.card__armor,
  dim__cards.card__artist_name,
  dim__cards.card__attack,
  dim__cards.card__banned_from_sideboard,
  dim__cards.card__collectible,
  dim__cards.card__crop_image,
  dim__cards.card__durability,
  dim__cards.card__flavor_text,
  dim__cards.card__health,
  dim__cards.card__image,
  dim__cards.card__image_gold,
  dim__cards.card__name,
  dim__cards.card__slug,
  dim__cards.card__text,
  dim__cards.card__extracted_at,
  dim__cards.card__loaded_at,
  dim__cards.card__hash_diff,
  dim__cards.card__version,
  dim__cards.card__valid_from,
  dim__cards.card__valid_to,
  dim__cards.card__is_current_record,
  dim__classes.class_slug,
  dim__classes.class_name,
  dim__classes.class__extracted_at,
  dim__classes.class__loaded_at,
  dim__classes.class__hash_diff,
  dim__classes.class__version,
  dim__classes.class__valid_from,
  dim__classes.class__valid_to,
  dim__classes.class__is_current_record,
  dim__keywords.keyword_id,
  dim__keywords.keyword__slug,
  dim__keywords.keyword__name,
  dim__keywords.keyword__ref_text,
  dim__keywords.keyword__text,
  dim__keywords.keyword__game_modes,
  dim__keywords.keyword__extracted_at,
  dim__keywords.keyword__loaded_at,
  dim__keywords.keyword__hash_diff,
  dim__keywords.keyword__version,
  dim__keywords.keyword__valid_from,
  dim__keywords.keyword__valid_to,
  dim__keywords.keyword__is_current_record,
  dim__minion_types.minion_type_slug,
  dim__minion_types.minion_type_name,
  dim__minion_types.minion_type__extracted_at,
  dim__minion_types.minion_type__loaded_at,
  dim__minion_types.minion_type__hash_diff,
  dim__minion_types.minion_type__version,
  dim__minion_types.minion_type__valid_from,
  dim__minion_types.minion_type__valid_to,
  dim__minion_types.minion_type__is_current_record,
  dim__rarities.rarity_slug,
  dim__rarities.rarity_name,
  dim__rarities.rarity_crafting_cost,
  dim__rarities.rarity_dust_value,
  dim__rarities.rarity__extracted_at,
  dim__rarities.rarity__loaded_at,
  dim__rarities.rarity__hash_diff,
  dim__rarities.rarity__version,
  dim__rarities.rarity__valid_from,
  dim__rarities.rarity__valid_to,
  dim__rarities.rarity__is_current_record,
  dim__related_cards.related_card__armor,
  dim__related_cards.related_card__artist_name,
  dim__related_cards.related_card__attack,
  dim__related_cards.related_card__banned_from_sideboard,
  dim__related_cards.related_card__collectible,
  dim__related_cards.related_card__crop_image,
  dim__related_cards.related_card__durability,
  dim__related_cards.related_card__flavor_text,
  dim__related_cards.related_card__health,
  dim__related_cards.related_card__image,
  dim__related_cards.related_card__image_gold,
  dim__related_cards.related_card__name,
  dim__related_cards.related_card__slug,
  dim__related_cards.related_card__text,
  dim__related_cards.related_card__extracted_at,
  dim__related_cards.related_card__loaded_at,
  dim__related_cards.related_card__hash_diff,
  dim__related_cards.related_card__version,
  dim__related_cards.related_card__valid_from,
  dim__related_cards.related_card__valid_to,
  dim__related_cards.related_card__is_current_record,
  dim__related_classes.related_class_slug,
  dim__related_classes.related_class_name,
  dim__related_classes.related_class__extracted_at,
  dim__related_classes.related_class__loaded_at,
  dim__related_classes.related_class__hash_diff,
  dim__related_classes.related_class__version,
  dim__related_classes.related_class__valid_from,
  dim__related_classes.related_class__valid_to,
  dim__related_classes.related_class__is_current_record,
  dim__related_types.related_type_slug,
  dim__related_types.related_type_name,
  dim__related_types.related_type__extracted_at,
  dim__related_types.related_type__loaded_at,
  dim__related_types.related_type__hash_diff,
  dim__related_types.related_type__version,
  dim__related_types.related_type__valid_from,
  dim__related_types.related_type__valid_to,
  dim__related_types.related_type__is_current_record,
  dim__sets.set__name,
  dim__sets.set__slug,
  dim__sets.set__hyped,
  dim__sets.set__type,
  dim__sets.set__collectible_count,
  dim__sets.set__collectible_revealed_count,
  dim__sets.set__non_collectible_count,
  dim__sets.set__non_collectible_revealed_count,
  dim__sets.set__extracted_at,
  dim__sets.set__loaded_at,
  dim__sets.set__hash_diff,
  dim__sets.set__version,
  dim__sets.set__valid_from,
  dim__sets.set__valid_to,
  dim__sets.set__is_current_record,
  dim__spell_schools.spell_school_pit_hk,
  dim__spell_schools.spell_school_id,
  dim__spell_schools.spell_school__name,
  dim__spell_schools.spell_school__slug,
  dim__spell_schools.spell_school__extracted_at,
  dim__spell_schools.spell_school__loaded_at,
  dim__spell_schools.spell_school__hash_diff,
  dim__spell_schools.spell_school__version,
  dim__spell_schools.spell_school__valid_from,
  dim__spell_schools.spell_school__valid_to,
  dim__spell_schools.spell_school__is_current_record,
  dim__tourist_classes.tourist_class_slug,
  dim__tourist_classes.tourist_class_name,
  dim__tourist_classes.tourist_class__extracted_at,
  dim__tourist_classes.tourist_class__loaded_at,
  dim__tourist_classes.tourist_class__hash_diff,
  dim__tourist_classes.tourist_class__version,
  dim__tourist_classes.tourist_class__valid_from,
  dim__tourist_classes.tourist_class__valid_to,
  dim__tourist_classes.tourist_class__is_current_record,
  dim__types.type_slug,
  dim__types.type_name,
  dim__types.type__extracted_at,
  dim__types.type__loaded_at,
  dim__types.type__hash_diff,
  dim__types.type__version,
  dim__types.type__valid_from,
  dim__types.type__valid_to,
  dim__types.type__is_current_record,
  fact__cards.is_zilliax_cosmetic_module,
  fact__cards.is_zilliax_functional_module,
  fact__cards.mana_cost,
  fact__cards.blood_rune_cost,
  fact__cards.frost_rune_cost,
  fact__cards.unholy_rune_cost,
  fact__cards.total_rune_cost,
  fact__cards.fact__extracted_at,
  fact__cards.fact__loaded_at,
  fact__cards.fact__version,
  fact__cards.fact__valid_from,
  fact__cards.fact__valid_to,
  fact__cards.fact__is_current_record
FROM gold.mart__cards.fact__cards
LEFT JOIN gold.mart__cards.dim__cardbacks
  ON fact__cards.cardback_pit_hk = dim__cardbacks.cardback_pit_hk
LEFT JOIN gold.mart__cards.dim__cards
  ON fact__cards.card_pit_hk = dim__cards.card_pit_hk
LEFT JOIN gold.mart__cards.dim__classes
  ON fact__cards.class_pit_hk = dim__classes.class_pit_hk
LEFT JOIN gold.mart__cards.link__keywords
  ON fact__cards.fact_record_hk = link__keywords.fact_record_hk
LEFT JOIN gold.mart__cards.dim__keywords
  ON link__keywords.keyword_pit_hk = dim__keywords.keyword_pit_hk
LEFT JOIN gold.mart__cards.dim__minion_types
  ON fact__cards.minion_type_pit_hk = dim__minion_types.minion_type_pit_hk
LEFT JOIN gold.mart__cards.dim__rarities
  ON fact__cards.rarity_pit_hk = dim__rarities.rarity_pit_hk
LEFT JOIN gold.mart__cards.link__related_cards
  ON fact__cards.fact_record_hk = link__related_cards.fact_record_hk
LEFT JOIN gold.mart__cards.dim__related_cards
  ON link__related_cards.related_card_pit_hk = dim__related_cards.related_card_pit_hk
LEFT JOIN gold.mart__cards.link__related_classes
  ON fact__cards.fact_record_hk = link__related_classes.fact_record_hk
LEFT JOIN gold.mart__cards.dim__related_classes
  ON link__related_classes.related_class_pit_hk = dim__related_classes.related_class_pit_hk
LEFT JOIN gold.mart__cards.link__related_types
  ON fact__cards.fact_record_hk = link__related_types.fact_record_hk
LEFT JOIN gold.mart__cards.dim__related_types
  ON link__related_types.related_type_pit_hk = dim__related_types.related_type_pit_hk
LEFT JOIN gold.mart__cards.dim__sets
  ON fact__cards.set_pit_hk = dim__sets.set_pit_hk
LEFT JOIN gold.mart__cards.dim__spell_schools
ON fact__cards.spell_school_pit_hk = dim__spell_schools.spell_school_pit_hk
LEFT JOIN gold.mart__cards.dim__tourist_classes
  ON fact__cards.tourist_class_pit_hk = dim__tourist_classes.tourist_class_pit_hk
LEFT JOIN gold.mart__cards.dim__types
  ON fact__cards.type_pit_hk = dim__types.type_pit_hk
WHERE
  fact__loaded_at BETWEEN @start_ts AND @end_ts