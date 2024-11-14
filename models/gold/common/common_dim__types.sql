/* Dimension of all the types in Hearthstone */
MODEL (
  name gold.common.common_dim__types,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (type__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__types
), final AS (
  SELECT
    type_pit_hk, /* Unique identifier in time for the class */
    type_slug AS type_slug, /* Slug of the class */
    type_id AS type_id, /* Unique identifier for the class */
    type_name AS type_name, /* Name of the class */
    _sqlmesh__extracted_at AS type__extracted_at, /* Timestamp when the class was extracted */
    _sqlmesh__loaded_at AS type__loaded_at, /* Timestamp when the class was loaded */
    _sqlmesh__hash_diff AS type__hash_diff, /* Hash diff of the class */
    _sqlmesh__version AS type__version, /* Record version of the class */
    _sqlmesh__valid_from AS type__valid_from, /* Class valid from timestamp */
    _sqlmesh__valid_to AS type__valid_to, /* Class valid to timestamp */
    _sqlmesh__is_current_record AS type__is_current_record /* Whether the class is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  type__loaded_at BETWEEN @start_ts AND @end_ts