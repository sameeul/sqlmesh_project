MODEL (
  name pcornet.aux_concept_relationship,
  kind FULL,
  dialect spark,
  tags ['step04_aux'],
);

SELECT * 
FROM read_parquet(`models/step04_aux/step04_aux_files/omop/concept_relationship.parquet`);