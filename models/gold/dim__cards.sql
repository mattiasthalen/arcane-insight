MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (card__loaded_at, '%Y-%m-%d %H:%M:%S')
  )
);

WITH source AS (
  SELECT
    *
  FROM silver.stg__hearthstone__cards
), final AS (
  SELECT
    card_id,
    armor AS card__armor,
    artist_name AS card__artist_name,
    attack AS card__attack,
    banned_from_sideboard AS card__banned_from_sideboard,
    collectible AS card__collectible,
    crop_image AS card__crop_image,
    durability AS card__durability,
    flavor_text AS card__flavor_text,
    health AS card__health,
    image AS card__image,
    image_gold AS card__image_gold,
    is_zilliax_cosmetic_module AS card__is_zilliax_cosmetic_module,
    is_zilliax_functional_module AS card__is_zilliax_functional_module,
    card_name AS card__name,
    card_slug AS card__slug,
    card_text AS card__text,
    _sqlmesh__extracted_at AS card__extracted_at,
    _sqlmesh__loaded_at AS card__loaded_at,
    _sqlmesh__hash_diff AS card__hash_diff,
    _sqlmesh__record_version AS card__record_version,
    _sqlmesh__record_valid_from AS card__record_valid_from,
    _sqlmesh__record_valid_to AS card__record_valid_to,
    _sqlmesh__is_current_record AS card__is_current_record
  FROM source
)
SELECT
  *
FROM final
WHERE
  card__loaded_at BETWEEN @start_ts AND @end_ts