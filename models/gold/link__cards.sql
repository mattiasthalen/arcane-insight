MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.stg__hearthstone__cards
), unnested AS (
  SELECT
    card_relations,
    _sqlmesh__loaded_at,
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
), final AS (
  SELECT
    unpivoted.card_relations,
    unpivoted._sqlmesh__loaded_at,
    unpivoted.card_relation,
    source.card_pit_hk
  FROM unpivoted
  LEFT JOIN source
    ON unpivoted.card_id = source.card_id
    AND unpivoted._sqlmesh__loaded_at BETWEEN source._sqlmesh__record_valid_from AND source._sqlmesh__record_valid_to
)
SELECT
  *
FROM final
WHERE
  _sqlmesh__loaded_at BETWEEN @start_ts AND @end_ts