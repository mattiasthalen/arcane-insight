/* Dimension of all the rarities in Hearthstone */
MODEL (
  name gold.mart__cards.dim__rarities,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__rarities;

@export_to_parquet(@this_model, "data")