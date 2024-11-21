/* Data vault staging model for the Hearthstone cards */
MODEL (
  name silver.data_vault.dv_stg__hearthstone__cards,
  kind FULL
);

@data_vault__staging(
  source := bronze.snapshot.snp__hearthstone__cards,
  lookup_data := (
    class_slug := (
      lookup_column := slug,
      lookup_table := bronze.snapshot.snp__hearthstone__classes,
      left_column := classId,
      right_column := id
    ),
    type_slug := (
      lookup_column := slug,
      lookup_table := bronze.snapshot.snp__hearthstone__types,
      left_column := typeId,
      right_column := id
    )
  ),
  derived_columns := (card_bk := slug || '|' || id, class_bk := class_slug, type_bk := type_slug),
  hashes := (
    card_hk := card_bk,
    class_hk := class_bk,
    type_hk := type_bk,
    card_hk__class_hk := (card_bk, class_bk),
    card_hk__type_hk := (card_bk, type_bk),
    card__pit_hk := (card_bk, _sqlmesh__vaid_from)
  ),
  source_system := 'hearthstone',
  loaded_at := _sqlmesh__loaded_at,
  valid_from := _sqlmesh__valid_from,
  valid_to := _sqlmesh__valid_to,
  hash_function := 'SHA256'
)