MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    card_relations,
    _sqlmesh__loaded_at
  FROM silver.stg__hearthstone__cards
), unnested AS (
  SELECT
    *,
    UNNEST(card_relations.child_card_ids) AS child_card_id,
    card_relations.card_id,
    card_relations.copy_of_card_id,
    card_relations.parent_card_id
  FROM source
), final AS (
  SELECT DISTINCT
    *
  FROM unnested
  UNPIVOT(card_id FOR type IN (card_id, copy_of_card_id, parent_card_id, child_card_id))
  ORDER BY
    type,
    card_id
)
SELECT
  *
FROM final
WHERE
  _sqlmesh__loaded_at BETWEEN @start_ts AND @end_ts