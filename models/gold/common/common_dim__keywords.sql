/* Dimension of all the keywords in Hearthstone */
MODEL (
  name gold.common.common_dim__keywords,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (keyword__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__keywords
), final AS (
  SELECT
    keyword_pit_hk, /* Unique identifier in time for the keyword */
    keyword_id AS keyword_id, /* Unique identifier for the keyword */
    slug AS keyword__slug, /* Slug of the keyword */
    name AS keyword__name, /* Name of the keyword */
    ref_text AS keyword__ref_text, /* Reference text of the keyword */
    text AS keyword__text, /* Text of the keyword */
    game_modes AS keyword__game_modes, /* Game modes of the keyword */
    _sqlmesh__extracted_at AS keyword__extracted_at, /* Timestamp when the keyword was extracted */
    _sqlmesh__loaded_at AS keyword__loaded_at, /* Timestamp when the keyword was loaded */
    _sqlmesh__hash_diff AS keyword__hash_diff, /* Hash diff of the keyword */
    _sqlmesh__version AS keyword__version, /* Record version of the keyword */
    _sqlmesh__valid_from AS keyword__valid_from, /* Card valid from timestamp */
    _sqlmesh__valid_to AS keyword__valid_to, /* Card valid to timestamp */
    _sqlmesh__is_current_record AS keyword__is_current_record /* Whether the keyword is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  keyword__loaded_at BETWEEN @start_ts AND @end_ts