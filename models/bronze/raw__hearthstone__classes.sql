MODEL (
kind FULL,
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
