/* Dimension of all the classes in Hearthstone */
MODEL (
  name gold.common.common_dim__classes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (class__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__classes
), final AS (
  SELECT
    class_pit_hk, /* Unique identifier in time for the class */
    class_slug AS class_slug, /* Slug of the class */
    class_id AS class_id, /* Unique identifier for the class */
    class_name AS class_name, /* Name of the class */
    _dlt_extracted_at AS class__extracted_at, /* Timestamp when the class was extracted */
    _sqlmesh_loaded_at AS class__loaded_at, /* Timestamp when the class was loaded */
    _sqlmesh_hash_diff AS class__hash_diff, /* Hash diff of the class */
    _sqlmesh_version AS class__version, /* Record version of the class */
    _sqlmesh_valid_from AS class__valid_from, /* Class valid from timestamp */
    _sqlmesh_valid_to AS class__valid_to, /* Class valid to timestamp */
    _sqlmesh_is_current_record AS class__is_current_record /* Whether the class is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  class__loaded_at BETWEEN @start_ts AND @end_ts