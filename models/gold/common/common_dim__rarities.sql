/* Dimension of all the rarities in Hearthstone */
MODEL (
  name gold.common.common_dim__rarities,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (rarity__loaded_at, '%Y-%m-%d %H:%M:%S')
  ),
  allow_partials TRUE
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__rarities
), final AS (
  SELECT
    rarity_pit_hk, /* Unique identifier in time for the rarity */
    rarity_slug, /* Slug of the rarity */
    rarity_id, /* Unique identifier for the rarity */
    rarity_name, /* Name of the rarity */
    crafting_cost AS rarity_crafting_cost, /* Crafting cost of the rarity */
    dust_value AS rarity_dust_value, /* Dust value of the rarity */
    _sqlmesh__extracted_at AS rarity__extracted_at, /* Timestamp when the rarity was extracted */
    _sqlmesh__loaded_at AS rarity__loaded_at, /* Timestamp when the rarity was loaded */
    _sqlmesh__hash_diff AS rarity__hash_diff, /* Hash diff of the rarity */
    _sqlmesh__version AS rarity__version, /* Record version of the rarity */
    _sqlmesh__valid_from AS rarity__valid_from, /* Type valid from timestamp */
    _sqlmesh__valid_to AS rarity__valid_to, /* Type valid to timestamp */
    _sqlmesh__is_current_record AS rarity__is_current_record /* Whether the rarity is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  rarity__loaded_at BETWEEN @start_ts AND @end_ts