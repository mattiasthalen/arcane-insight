MODEL (
  name battle_net_sqlmesh.incremental_raw_hearthstone_classes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  columns (
    slug TEXT,
    id BIGINT,
    name TEXT,
    card_id BIGINT,
    hero_power_card_id BIGINT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    _dlt_load_time TIMESTAMP
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
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  battle_net.raw_hearthstone_classes
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
