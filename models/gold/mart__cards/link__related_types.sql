/* Link between fact and dimension for all Hearthstone related types */
MODEL (
  name gold.mart__cards.link__related_types,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (link__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH fact__cards AS (
  SELECT
    *
  FROM gold.mart__cards.fact__cards
), dim__related_types AS (
  SELECT
    *
  FROM gold.mart__cards.dim__related_types
), fact__unnested AS (
  SELECT
    fact_record_hk,
    fact__extracted_at,
    fact__loaded_at,
    fact__valid_from,
    fact__valid_to,
    UNNEST(multi_class_ids) AS related_type_id
  FROM fact__cards
), fact__aggregated AS (
  SELECT
    fact_record_hk,
    related_type_id,
    MAX(fact__extracted_at) AS link__extracted_at,
    MAX(fact__loaded_at) AS link__loaded_at,
    MAX(fact__valid_from) AS link__valid_from,
    MIN(fact__valid_to) AS link__valid_to
  FROM fact__unnested
  GROUP BY ALL
), final AS (
  SELECT
    fact__aggregated.fact_record_hk, /* Unique identifier for the fact record */
    dim__related_types.related_type_pit_hk, /* Unique identifier in time for the related class */
    fact__aggregated.link__extracted_at, /* Time when the link was extracted */
    fact__aggregated.link__loaded_at, /* Time when the link was loaded */
    fact__aggregated.link__valid_from, /* Time when the link is valid from */
    fact__aggregated.link__valid_to /* Time when the link is valid to */
  FROM fact__aggregated
  LEFT JOIN dim__related_types
    ON fact__aggregated.related_type_id = dim__related_types.related_type_id
    AND fact__aggregated.link__loaded_at BETWEEN dim__related_types.related_type__valid_from AND dim__related_types.related_type__valid_from
)
SELECT
  *
FROM final
WHERE
  link__loaded_at BETWEEN @start_ts AND @end_ts;

@export_to_parquet(@this_model, "data")