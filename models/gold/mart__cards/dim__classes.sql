/* Dimension of all the classes in Hearthstone */
MODEL (
  name gold.mart__cards.dim__classes,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__classes
;

@export_to_parquet(@this_model, "data")