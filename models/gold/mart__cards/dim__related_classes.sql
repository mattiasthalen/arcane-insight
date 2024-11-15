/* Dimension of all the related classes in Hearthstone */
MODEL (
  name gold.mart__cards.dim__related_classes,
  kind VIEW
);

SELECT
  @star(common_dim__classes, prefix := 'related_')
FROM gold.common.common_dim__classes;

@export_to_parquet(@this_model, "data")