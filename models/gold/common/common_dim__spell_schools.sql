/* Dimension of all the spell schools in Hearthstone */
MODEL (
  name gold.common.common_dim__spell_schools,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (spell_school__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__spell_schools
), final AS (
  SELECT
    spell_school_pit_hk, /* Unique identifier in time for the spell school */
    spell_school_id, /* Unique identifier for the spell school */
    name AS spell_school__name, /* Name of the spell school */
    slug AS spell_school__slug, /* Slug of the spell school */
    _sqlmesh__extracted_at AS spell_school__extracted_at, /* Timestamp when the spell school was extracted */
    _sqlmesh__loaded_at AS spell_school__loaded_at, /* Timestamp when the spell school was loaded */
    _sqlmesh__hash_diff AS spell_school__hash_diff, /* Hash diff of the spell school */
    _sqlmesh__version AS spell_school__version, /* Record version of the spell school */
    _sqlmesh__valid_from AS spell_school__valid_from, /* Spell school valid from timestamp */
    _sqlmesh__valid_to AS spell_school__valid_to, /* Spell school valid to timestamp */
    _sqlmesh__is_current_record AS spell_school__is_current_record /* Whether the spell school is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  spell_school__loaded_at BETWEEN @start_ts AND @end_ts