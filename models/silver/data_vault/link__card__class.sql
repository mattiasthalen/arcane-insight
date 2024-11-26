/* Link model of the card to class from Hearthstone */
MODEL (
enabled false,
  name silver.data_vault.link__card__class,
  cron "*/5 * * * *",
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key card_hk__class_hk,
    columns [card_hk__class_hk],
    valid_from_name _sqlmesh__valid_from,
    valid_to_name _sqlmesh__valid_to
  )
);

SELECT
    dv_stg__hearthstone__cards.card_hk,
    dv_stg__hearthstone__classes.class_hk,
    @generate_surrogate_key__sha_256(card_hk, card_hk) AS card_hk__class_hk,
    @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
FROM silver.data_vault.dv_stg__hearthstone__cards
LEFT JOIN silver.data_vault.dv_stg__hearthstone__classes
    ON dv_stg__hearthstone__cards.class_id = dv_stg__hearthstone__classes.class_id
ORDER BY
    dv_stg__hearthstone__cards._sqlmesh__extracted_at
