MODEL (
  name bronze_sqlmesh.incremental_raw_hearthstone_set_groups__card_sets,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_loaded_at,
  ),
  columns (
    value TEXT,
    _dlt_parent_id TEXT,
    _dlt_list_idx BIGINT,
    _dlt_id TEXT,
    _dlt_loaded_at TIMESTAMP
  ),
);

SELECT
  value,
  _dlt_parent_id,
  _dlt_list_idx,
  _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_loaded_at
FROM
  bronze.raw_hearthstone_set_groups__card_sets
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
