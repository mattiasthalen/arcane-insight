MODEL (
  name bronze_sqlmesh.incremental_raw_hearthstone_classes__alternate_hero_card_ids,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_loaded_at,
  ),
  columns (
    value BIGINT,
    _dlt_parent_id TEXT,
    _dlt_list_idx BIGINT,
    _dlt_id TEXT
  ),
);

SELECT
  value,
  _dlt_parent_id,
  _dlt_list_idx,
  _dlt_id
FROM
  bronze.raw_hearthstone_classes__alternate_hero_card_ids
