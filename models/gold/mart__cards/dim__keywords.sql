/* Dimension of all the keywords in Hearthstone */
MODEL (
  name gold.mart__cards.dim__keywords,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__keywords;

@export_to_parquet(@this_model, "data")