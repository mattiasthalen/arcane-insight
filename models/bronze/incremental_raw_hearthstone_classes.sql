MODEL (
  name bronze_sqlmesh.incremental_raw_hearthstone_classes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_loaded_at,
  ),
  columns (
    slug TEXT,
    id BIGINT,
    name TEXT,
    card_id BIGINT,
    hero_power_card_id BIGINT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    _dlt_loaded_at TIMESTAMP
  ),
);

SELECT
  slug,
  id,
  name,
  card_id,
  hero_power_card_id,
  _dlt_load_id,
  _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_loaded_at
FROM
  bronze.raw_hearthstone_classes
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
