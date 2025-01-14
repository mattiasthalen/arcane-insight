/* Dimension of all the minion types in Hearthstone */
MODEL (
  name gold.common.common_dim__minion_types,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (minion_type__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__minion_types
), final AS (
  SELECT
    minion_type_pit_hk, /* Unique identifier in time for the minion type */
    minion_type_slug, /* Slug of the minion type */
    minion_type_id, /* Unique identifier for the minion type */
    minion_type_name, /* Name of the minion type */
    _dlt_extracted_at AS minion_type__extracted_at, /* Timestamp when the minion type was extracted */
    _sqlmesh_loaded_at AS minion_type__loaded_at, /* Timestamp when the minion type was loaded */
    _sqlmesh_hash_diff AS minion_type__hash_diff, /* Hash diff of the minion type */
    _sqlmesh_version AS minion_type__version, /* Record version of the minion type */
    _sqlmesh_valid_from AS minion_type__valid_from, /* Minion type valid from timestamp */
    _sqlmesh_valid_to AS minion_type__valid_to, /* Minion type valid to timestamp */
    _sqlmesh_is_current_record AS minion_type__is_current_record /* Whether the minion type is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  minion_type__loaded_at BETWEEN @start_ts AND @end_ts