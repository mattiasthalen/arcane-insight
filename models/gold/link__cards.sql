MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (link__record_valid_from, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.stg__hearthstone__cards
), unnested AS (
  SELECT
    card_relations,
    _sqlmesh__record_valid_from,
    _sqlmesh__record_valid_to,
    UNNEST(card_relations.child_card_ids) AS child_card_id,
    card_relations.card_id,
    card_relations.copy_of_card_id,
    card_relations.parent_card_id
  FROM source
), unpivoted AS (
  SELECT DISTINCT
    *
  FROM unnested
  UNPIVOT(card_id FOR card_relation IN (card_id, copy_of_card_id, parent_card_id, child_card_id))
  ORDER BY
    card_relation,
    card_id
), aggregated AS (
  SELECT
    card_relations,
    card_relation,
    card_id,
    MAX(_sqlmesh__record_valid_from) AS link__record_valid_from,
    MIN(_sqlmesh__record_valid_to) AS link__record_valid_to
  FROM unpivoted
  GROUP BY ALL
), final AS (
  SELECT
    @generate_surrogate_key__sha_256(aggregated.card_relations, aggregated.link__record_valid_from) AS link_pit_hk,
    aggregated.card_relations,
    aggregated.card_relation,
    source.card_pit_hk,
    aggregated.link__record_valid_from,
    aggregated.link__record_valid_to
  FROM aggregated
  LEFT JOIN source
    ON aggregated.card_id = source.card_id
    AND aggregated.link__record_valid_from BETWEEN source._sqlmesh__record_valid_from AND source._sqlmesh__record_valid_to
)
SELECT
  *
FROM final
WHERE
  link__record_valid_from BETWEEN @start_ts AND @end_ts