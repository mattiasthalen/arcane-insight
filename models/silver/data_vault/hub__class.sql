/* Hub model of the classes from Hearthstone */
MODEL (
enabled false,
  name silver.data_vault.hub__class,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key class_bk,
    columns [class_bk],
    valid_from_name _sqlmesh__valid_from,
    valid_to_name _sqlmesh__valid_to
  )
);

SELECT
    class_hk,
    class_bk,
    @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
FROM silver.data_vault.dv_stg__hearthstone__classes
ORDER BY
    _sqlmesh__extracted_at