/* Satellite model of the classs from Hearthstone */
MODEL (
enabled FALSE,
  name silver.data_vault.sat__class,
  kind VIEW
);

SELECT
  class_hk,
  class_pit_hk,
  class_bk,
  class_id,
  slug,
  name,
  _sqlmesh__extracted_at,
  _sqlmesh__hash_diff,
  _sqlmesh__loaded_at,
  _sqlmesh__version,
  _sqlmesh__valid_from,
  _sqlmesh__valid_to,
  _sqlmesh__is_current_record
FROM silver.data_vault.dv_stg__hearthstone__classes
