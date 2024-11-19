/* Hub model of the cards from Hearthstone */
MODEL (
    enabled false,
  name silver.data_vault.hub__card,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key card_bk,
    columns [card_bk],
    valid_from_name _sqlmesh__valid_from,
    valid_to_name _sqlmesh__valid_to
  )
);

SELECT
    card_hk,
    card_bk,
    @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
FROM silver.data_vault.dv_stg__hearthstone__cards
ORDER BY
    _sqlmesh__extracted_at