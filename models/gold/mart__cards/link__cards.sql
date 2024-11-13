/* Link between fact and dimension for all Hearthstone card relationships */
MODEL (
  name gold.mart__cards.link__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (link__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH fact__cards AS (
  SELECT
    *
  FROM gold.mart__cards.fact__cards
), dim__cards AS (
  SELECT
    *
  FROM gold.mart__cards.dim__cards
), fact__unnested AS (
  SELECT
    fact_record_hk,
    fact__extracted_at,
    fact__loaded_at,
    fact__valid_from,
    fact__valid_to,
    UNNEST(card_relations.child_card_ids) AS child_card_id,
    card_relations.card_id,
    card_relations.copy_of_card_id,
    card_relations.parent_card_id
  FROM fact__cards
), fact__unpivoted AS (
  SELECT
    *
  FROM fact__unnested
  UNPIVOT(card_id FOR card_relation IN (card_id, copy_of_card_id, parent_card_id, child_card_id))
  ORDER BY
    fact_record_hk,
    card_id
), fact__aggregated AS (
  SELECT
    fact_record_hk,
    card_relation,
    card_id,
    MAX(fact__extracted_at) AS link__extracted_at,
    MAX(fact__loaded_at) AS link__loaded_at,
    MAX(fact__valid_from) AS link__valid_from,
    MIN(fact__valid_to) AS link__valid_to
  FROM fact__unpivoted
  GROUP BY ALL
), final AS (
  SELECT
    fact__aggregated.fact_record_hk, /* Unique identifier for the fact record */
    fact__aggregated.card_relation, /* Type of relationship */
    dim__cards.card_pit_hk, /* Unique identifier in time for the card */
    fact__aggregated.link__extracted_at, /* Time when the link was extracted */
    fact__aggregated.link__loaded_at, /* Time when the link was loaded */
    fact__aggregated.link__valid_from, /* Time when the link is valid from */
    fact__aggregated.link__valid_to /* Time when the link is valid to */
  FROM fact__aggregated
  LEFT JOIN dim__cards
    ON fact__aggregated.card_id = dim__cards.card_id
    AND fact__aggregated.link__loaded_at BETWEEN dim__cards.card__valid_from AND dim__cards.card__valid_from
)
SELECT
  *
FROM final
WHERE
  link__loaded_at BETWEEN @start_ts AND @end_ts