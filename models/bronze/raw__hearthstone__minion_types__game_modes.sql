MODEL (
  kind FULL,
  columns (
    value BIGINT,
    _dlt_parent_id TEXT,
    _dlt_list_idx BIGINT,
    _dlt_id TEXT
  )
);

SELECT
  value,
  _dlt_parent_id,
  _dlt_list_idx,
  _dlt_id
FROM bronze.raw_hearthstone_minion_types__game_modes