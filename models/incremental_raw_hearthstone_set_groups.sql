MODEL (
  name battle_net_sqlmesh.incremental_raw_hearthstone_set_groups,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  columns (
    slug TEXT,
    year BIGINT,
    svg TEXT,
    name TEXT,
    standard BOOLEAN,
    icon TEXT,
    _dlt_load_id TEXT,
    _dlt_id TEXT,
    year_range TEXT,
    _dlt_load_time TIMESTAMP
  ),
);

SELECT
  slug,
  year,
  svg,
  name,
  standard,
  icon,
  _dlt_load_id,
  _dlt_id,
  year_range,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  bronze.raw_hearthstone_set_groups
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
