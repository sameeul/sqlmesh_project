AUDIT (
  name row_count_gt_zero,
);

SELECT
  1 AS violation
WHERE NOT EXISTS (
  SELECT 1 FROM @this
);
