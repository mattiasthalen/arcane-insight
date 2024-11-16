/* Dimension of all the cardbacks in Hearthstone */
MODEL (
  name gold.common.common_dim__cardbacks,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (cardback__loaded_at, '%Y-%m-%d %H:%M:%S')
  ),
  allow_partials TRUE
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__cardbacks
), final AS (
  SELECT
    cardback_pit_hk, /* Unique identifier in time for the cardback */
    card_id, /* Unique identifier for the card */
    cardback_category, /* Category of the cardback */
    text AS cardback__text, /* Text of the cardback */
    name AS cardback__name, /* Name of the cardback */
    sort_category AS cardback__sort_category, /* Sort category of the cardback */
    slug AS cardback__slug, /* Slug of the cardback */
    image_url AS cardback__image_url, /* Image of the cardback */
    _sqlmesh__extracted_at AS cardback__extracted_at, /* Timestamp when the card was extracted */
    _sqlmesh__loaded_at AS cardback__loaded_at, /* Timestamp when the card was loaded */
    _sqlmesh__hash_diff AS cardback__hash_diff, /* Hash diff of the card */
    _sqlmesh__version AS cardback__version, /* Record version of the card */
    _sqlmesh__valid_from AS cardback__valid_from, /* Card valid from timestamp */
    _sqlmesh__valid_to AS cardback__valid_to, /* Card valid to timestamp */
    _sqlmesh__is_current_record AS cardback__is_current_record /* Whether the card is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  cardback__loaded_at BETWEEN @start_ts AND @end_ts