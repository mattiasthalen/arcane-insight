/* Data vault staging model for the Hearthstone cards */
MODEL (
  name silver.data_vault.dv_stg__hearthstone__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

SELECT
  *
FROM (
  @data_vault__staging(
    source := bronze.snapshot.snp__hearthstone__cards,
    lookup_data := (
      class_slug := (
        lookup_table := bronze.snapshot.snp__hearthstone__classes,
        lookup_column := slug,
        left_column := classId,
        right_column := id
      ),
      minion_slug := (
        lookup_table := bronze.snapshot.snp__hearthstone__minion_types,
        lookup_column := slug,
        left_column := minionTypeId,
        right_column := id
      ),
      rarity_slug := (
        lookup_table := bronze.snapshot.snp__hearthstone__rarities,
        lookup_column := slug,
        left_column := rarityId,
        right_column := id
      ),
      set_slug := (
        lookup_table := bronze.snapshot.snp__hearthstone__sets,
        lookup_column := slug,
        left_column := cardSetId,
        right_column := id
      ),
      spell_school_slug := (
        lookup_table := bronze.snapshot.snp__hearthstone__spell_schools,
        lookup_column := slug,
        left_column := spellSchoolId,
        right_column := id
      ),
      type_slug := (
        lookup_table := bronze.snapshot.snp__hearthstone__types,
        lookup_column := slug,
        left_column := cardTypeId,
        right_column := id
      )
    ),
    derived_columns := (
      card_bk := slug,
      class_bk := class_slug,
      minion_bk := minion_slug,
      rarity_bk := rarity_slug,
      set_bk := set_slug,
      spell_school_bk := spell_school_slug,
      type_bk := type_slug
    ),
    hashes := (
      card_hk := card_bk,
      class_hk := class_bk,
      minion_hk := minion_bk,
      rarity_hk := rarity_bk,
      set_hk := set_bk,
      spell_school_hk := spell_school_bk,
      type_hk := type_bk,
      card_hk__class_hk := (card_bk, class_bk),
      card_hk__minion_hk := (card_bk, minion_bk),
      card_hk__rarity_hk := (card_bk, rarity_bk),
      card_hk__set_hk := (card_bk, set_bk),
      card_hk__spell_school_hk := (card_bk, spell_school_bk),
      card_hk__type_hk := (card_bk, type_bk),
      card__pit_hk := (card_bk, _sqlmesh__valid_from)
    ),
    source_system := 'hearthstone',
    loaded_at := _sqlmesh__loaded_at,
    valid_from := _sqlmesh__valid_from,
    valid_to := _sqlmesh__valid_to,
    generate_ghost_record := TRUE,
    hash_function := 'SHA256'
  )
)
WHERE
  _sqlmesh__loaded_at BETWEEN @start_ts AND @end_ts