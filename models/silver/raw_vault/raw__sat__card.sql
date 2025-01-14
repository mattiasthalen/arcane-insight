/* Data vault satellite model for the Hearthstone cards */
MODEL (
  name silver.raw_vault.raw__sat__card,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key hash_key__card_bk,
    columns [_sqlmesh_hash_diff],
    valid_to_name _sqlmesh_valid_to,
    valid_from_name _sqlmesh_valid_from
  ),
  allow_partials TRUE
);

SELECT
  hash_key__card_bk,
  id,
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
  is_zilliax_cosmetic_module,
  is_zilliax_functional_module,
  mana_cost,
  max_sideboard_cards,
  name,
  rune_cost,
  slug,
  text,
  _sqlmesh_hash_diff,
  _sqlmesh_record_source,
  _dlt_extracted_at,
  _sqlmesh_loaded_at
FROM silver.staging.dv_stg__hearthstone__cards