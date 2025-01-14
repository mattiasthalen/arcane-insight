/* Dimension of all the sets in Hearthstone */
MODEL (
  name gold.common.common_dim__sets,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (set__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__sets
), final AS (
  SELECT
    set_pit_hk, /* Unique identifier in time for the set */
    set_id, /* Unique identifier for the set */
    name AS set__name, /* Name of the set */
    slug AS set__slug, /* Slug of the set */
    hyped AS set__hyped, /* Whether the set is hyped */
    type AS set__type, /* Type of the set */
    collectible_count AS set__collectible_count, /* Count of collectible cards in the set */
    collectible_revealed_count AS set__collectible_revealed_count, /* Count of collectible cards revealed in the set */
    non_collectible_count AS set__non_collectible_count, /* Non-collectible cards in the set */
    non_collectible_revealed_count AS set__non_collectible_revealed_count, /* Non-collectible cards revealed in the set */
    _dlt_extracted_at AS set__extracted_at, /* Timestamp when the set was extracted */
    _sqlmesh_loaded_at AS set__loaded_at, /* Timestamp when the set was loaded */
    _sqlmesh_hash_diff AS set__hash_diff, /* Hash diff of the set */
    _sqlmesh_version AS set__version, /* Record version of the set */
    _sqlmesh_valid_from AS set__valid_from, /* Card valid from timestamp */
    _sqlmesh_valid_to AS set__valid_to, /* Card valid to timestamp */
    _sqlmesh_is_current_record AS set__is_current_record /* Whether the set is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  set__loaded_at BETWEEN @start_ts AND @end_ts