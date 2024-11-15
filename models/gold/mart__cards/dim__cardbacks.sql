/* Dimension of all the cardbacks in Hearthstone */
MODEL (
  name gold.mart__cards.dim__cardbacks,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__cardbacks;

@export_to_parquet(@this_model, "data")