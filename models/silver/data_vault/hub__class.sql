/* Hub model of the classes from Hearthstone */
MODEL (
  name silver.data_vault.hub__class,
  kind FULL
);

@data_vault__hub(
    sources := [
        silver.data_vault.dv_stg__hearthstone__classes := (
            class_bk,
            class_hk
        ),
        silver.data_vault.dv_stg__hearthstone__cards := (
            class_bk,
            class_hk
        )
    ],
    business_key := class_bk,
    source_system := source_system,
    source_table := source_table,
    loaded_at := _sqlmesh__loaded_at
)