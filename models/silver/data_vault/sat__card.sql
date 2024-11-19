/* Satellite model of the cards from Hearthstone */
MODEL (
  enabled FALSE,
  name silver.data_vault.sat__card,
  kind VIEW
);

SELECT
  card_hk,
  card_pit_hk,
  card_bk,
  card_id,
  armor,
  artist_name,
  attack,
  banned_from_sideboard,
  collectible,
  crop_image,
  durability,
  flavor_text,
  health,
  image,
  image_gold,
  name,
  slug,
  text,
  _sqlmesh__extracted_at,
  _sqlmesh__hash_diff,
  _sqlmesh__loaded_at,
  _sqlmesh__version,
  _sqlmesh__valid_from,
  _sqlmesh__valid_to,
  _sqlmesh__is_current_record
FROM silver.data_vault.dv_stg__hearthstone__cards
