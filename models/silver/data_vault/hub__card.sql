/* Hub model of the cards from Hearthstone */
MODEL (
  name silver.data_vault.hub__card,
  kind FULL
);

@data_vault__hub(
    sources := [
        silver.data_vault.dv_stg__hearthstone__cards := (
            card_bk,
            card_hk
        ),
        silver.data_vault.dv_stg__hearthstone__classes := (
            card_bk,
            card_hk
        ),
        silver.data_vault.dv_stg__hearthstone__classes := (
            hero_power_card_bk card_bk,
            hero_power_card_hk card_hk
        )
    ],
    business_key := card_bk,
    source_system := source_system,
    source_table := source_table,
    loaded_at := _sqlmesh__loaded_at
)