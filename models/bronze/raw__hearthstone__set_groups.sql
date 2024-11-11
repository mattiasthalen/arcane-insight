MODEL (
kind FULL,
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
    _dlt_loaded_at TIMESTAMP
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
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_loaded_at
FROM
  bronze.raw_hearthstone_set_groups
