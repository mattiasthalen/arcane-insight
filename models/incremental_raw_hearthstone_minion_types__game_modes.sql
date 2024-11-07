MODEL (
  name battle_net_sqlmesh.incremental_raw_hearthstone_minion_types__game_modes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),
  columns (
    value BIGINT,
    _dlt_parent_id TEXT,
    _dlt_list_idx BIGINT,
    _dlt_id TEXT,
    _dlt_load_time TIMESTAMP
  ),
);

SELECT
  value,
  _dlt_parent_id,
  _dlt_list_idx,
  _dlt_id,
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) as _dlt_load_time
FROM
  battle_net.raw_hearthstone_minion_types__game_modes
WHERE
  TO_TIMESTAMP(CAST(_dlt_load_id AS DOUBLE)) BETWEEN @start_ds AND @end_ds
