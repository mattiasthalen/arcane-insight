/* Dimension of all the sets in Hearthstone */
MODEL (
  name gold.mart__cards.dim__sets,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__sets;

@export_to_parquet(@this_model, "data")