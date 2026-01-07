MODEL (
  name pcornet.aux_concept,
  kind FULL,
  dialect spark,
  tags ['step04_aux'],
);

SELECT *
FROM parquet.`step04_aux_files/omop/concept.parquet`;
