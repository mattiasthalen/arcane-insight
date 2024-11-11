MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _sqlmesh_hashkey
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
    _dlt_loaded_at TIMESTAMP,
    _sqlmesh_hashkey BLOB,
    _sqlmesh_loaded_at TIMESTAMP
  )
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
  TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS _dlt_loaded_at,
  @generate_surrogate_key(slug, year, svg, name, standard, icon, year_range, _dlt_id) AS _sqlmesh_hashkey,
  @execution_ts::TIMESTAMP AS _sqlmesh_loaded_at
FROM bronze.raw_hearthstone_set_groups /* WHERE */ /*   TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds */