AUDIT (
  name schema_contains,
  dialect spark,
);

WITH expected AS (
  SELECT explode(split('{{ expected_columns }}', ',')) AS expected_col
),
actual AS (
  SELECT lower(column_name) AS column_name
  FROM information_schema.columns
  WHERE table_schema = '{{ this.schema }}'
    AND table_name = '{{ this.name }}'
)
SELECT
  expected_col AS missing_column
FROM expected
LEFT ANTI JOIN actual
  ON lower(expected_col) = actual.column_name;
