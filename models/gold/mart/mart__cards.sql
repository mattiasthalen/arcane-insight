/* Mart model of the cards from Hearthstone */
MODEL (
  name gold.mart.mart__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (fact__valid_from, '%Y-%m-%d %H:%M:%S')
  )
);

WITH fact__cards AS (
  SELECT
    *
  FROM gold.mart__cards.fact__cards
), link__cards AS (
  SELECT
    *
  FROM gold.mart__cards.link__cards
), dim__cards AS (
  SELECT
    *
  FROM gold.mart__cards.dim__cards
), dim__classes AS (
  SELECT
    *
  FROM gold.mart__cards.dim__classes
), final AS (
  SELECT
    fact__cards.fact_name,
    fact__cards.fact_record_hk,
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
  FROM fact__cards
  LEFT JOIN link__cards
    ON fact__cards.fact_record_hk = link__cards.fact_record_hk
  LEFT JOIN dim__cards
    ON link__cards.card_pit_hk = dim__cards.card_pit_hk
  LEFT JOIN dim__classes
    ON fact__cards.class_pit_hk = dim__classes.class_pit_hk
)
SELECT
  *
FROM final
WHERE
  fact__valid_from BETWEEN @start_ts AND @end_ts