/* Dimension of all the game modes in Hearthstone */
MODEL (
  name gold.common.common_dim__game_modes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (game_mode__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__game_modes
), final AS (
  SELECT
    game_mode_pit_hk, /* Unique identifier in time for the game mode */
    game_mode_id, /* Unique identifier for the game mode */
    name AS game_mode__name, /* Name of the game mode */
    slug AS game_mode__slug, /* Slug of the game mode */
    _sqlmesh__extracted_at AS game_mode__extracted_at, /* Timestamp when the game mode was extracted */
    _sqlmesh__loaded_at AS game_mode__loaded_at, /* Timestamp when the game mode was loaded */
    _sqlmesh__hash_diff AS game_mode__hash_diff, /* Hash diff of the game mode */
    _sqlmesh__version AS game_mode__version, /* Record version of the game mode */
    _sqlmesh__valid_from AS game_mode__valid_from, /* Game mode valid from timestamp */
    _sqlmesh__valid_to AS game_mode__valid_to, /* Game mode valid to timestamp */
    _sqlmesh__is_current_record AS game_mode__is_current_record /* Whether the game mode is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  game_mode__loaded_at BETWEEN @start_ts AND @end_ts