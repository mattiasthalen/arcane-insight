/* Data vault staging model for the Hearthstone classes */
MODEL (
  name silver.data_vault.dv_stg__hearthstone__classes,
  cron "*/5 * * * *",
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (_sqlmesh__loaded_at, '%Y-%m-%d %H:%M:%S')
  ),
  grain class_pit_hk
);

SELECT
  *
FROM (
  @data_vault__staging(
    source := bronze.snapshot.snp__hearthstone__classes,
    lookup_data := [
    card_bk := (
        lookup_table := bronze.snapshot.snp__hearthstone__cards,
        lookup_column := slug,
        left_column := cardId,
        right_column := id
      ),
      hero_power_card_bk := (
          lookup_table := bronze.snapshot.snp__hearthstone__cards,
          lookup_column := slug,
          left_column := heroPowerCardId,
          right_column := id
        )
    ],
    derived_columns := [
      class_bk := slug
    ],
    hashes := [
      class_pit_hk := (class_bk, _sqlmesh__valid_from),
      class_hk := class_bk,
      card_hk := card_bk,
      hero_power_card_hk := hero_power_card_bk,
      
      class_hk__card_hk := (class_bk, card_bk),
      class_hk__hero_power_card_hk := (class_bk, hero_power_card_bk),
      
      class_hash_diff := (
        slug,
        id,
        name,
        cardId,
        heroPowerCardId,
        alternateHeroCardIds
      )
      
    ],
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