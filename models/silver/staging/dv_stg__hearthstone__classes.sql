/* Data vault staging model for the Hearthstone classes */
MODEL (
  name silver.staging.dv_stg__hearthstone__classes,
  kind FULL
);

WITH source AS (
  SELECT
    *,
    (
      @execution_ts || '+00:00'
    )::TIMESTAMPTZ AS _sqlmesh__loaded_at
  FROM bronze.raw.raw__hearthstone__classes
), keys AS (
  SELECT
    *,
    id::TEXT AS class_id,
    slug::TEXT AS class_bk,
    cardId::TEXT AS card_bk,
    heroPowerCardId::TEXT AS hero_power_card_bk
  FROM source
), ghost_record AS (
  SELECT
    keys.*
    REPLACE (ghost._sqlmesh__record_source AS _sqlmesh__record_source, ghost._sqlmesh__loaded_at AS _sqlmesh__loaded_at)
  FROM (
    SELECT
      'GHOST_RECORD' AS _sqlmesh__record_source,
      '0001-01-01 00:00:00+00:00'::TIMESTAMPTZ AS _sqlmesh__loaded_at
  ) AS ghost
  LEFT JOIN keys
    ON ghost._sqlmesh__record_source = keys._sqlmesh__record_source
    AND ghost._sqlmesh__loaded_at = keys._sqlmesh__loaded_at
  UNION ALL
  SELECT
    *
  FROM keys
), hashes AS (
  SELECT
    *,
    @generate_surrogate_key(class_id, hash_function := 'SHA256') AS hash_key__class_id,
    @generate_surrogate_key(class_bk, hash_function := 'SHA256') AS hash_key__class_bk,
    @generate_surrogate_key(card_bk, hash_function := 'SHA256') AS hash_key__card_bk,
    @generate_surrogate_key(hero_power_card_bk, hash_function := 'SHA256') AS hash_key__hero_power_card_bk,
    @generate_surrogate_key(slug, id, name, hash_function := 'SHA256') AS hash_diff__class,
    @generate_surrogate_key(class_bk, card_bk, hash_function := 'SHA256') AS hash_key__class_bk__card_bk,
    @generate_surrogate_key(class_bk, hero_power_card_bk, hash_function := 'SHA256') AS hash_key__class_bk__hero_power_card_bk
  FROM ghost_record
)
SELECT
  *
FROM hashes