/* Dimension of all the cards in Hearthstone */
MODEL (
  name gold.common.common_dim__cards,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (card__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.staging.stg__hearthstone__cards
), final AS (
  SELECT
    card_pit_hk, /* Unique identifier in time for the card */
    card_id, /* Unique identifier for the card */
    armor AS card__armor, /* Armor value of the card */
    artist_name AS card__artist_name, /* Name of the artist who created the card */
    attack AS card__attack, /* Attack value of the card */
    banned_from_sideboard AS card__banned_from_sideboard, /* Whether the card is banned from the sideboard */
    collectible AS card__collectible, /* Whether the card is collectible */
    crop_image AS card__crop_image, /* URL to the cropped image of the card */
    durability AS card__durability, /* Durability value of the card */
    flavor_text AS card__flavor_text, /* Flavor text of the card */
    health AS card__health, /* Health value of the card */
    image AS card__image, /* URL to the image of the card */
    image_gold AS card__image_gold, /* URL to the golden image of the card */
    card_name AS card__name, /* Name of the card */
    card_slug AS card__slug, /* Slug of the card */
    card_text AS card__text, /* Text of the card */
    _sqlmesh__extracted_at AS card__extracted_at, /* Timestamp when the card was extracted */
    _sqlmesh__loaded_at AS card__loaded_at, /* Timestamp when the card was loaded */
    _sqlmesh__hash_diff AS card__hash_diff, /* Hash diff of the card */
    _sqlmesh__version AS card__version, /* Record version of the card */
    _sqlmesh__valid_from AS card__valid_from, /* Card valid from timestamp */
    _sqlmesh__valid_to AS card__valid_to, /* Card valid to timestamp */
    _sqlmesh__is_current_record AS card__is_current_record /* Whether the card is current */
  FROM source
)
SELECT
  *
FROM final
WHERE
  card__loaded_at BETWEEN @start_ts AND @end_ts