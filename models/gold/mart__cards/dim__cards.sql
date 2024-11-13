/* Dimension of all the cards in Hearthstone */
MODEL (
  name gold.mart__cards.dim__cards,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__cards;

@export_to_parquet(@this_model, "data")