/* Dimension of all the types in Hearthstone */
MODEL (
  name gold.mart__cards.dim__types,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__types;

@export_to_parquet(@this_model, "data")