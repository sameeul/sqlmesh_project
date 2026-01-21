MODEL (
  name pcornet.aux_concept_ancestor,
  kind FULL,
  dialect spark,

  tags ['step04_aux'],
);
SELECT * 
FROM read_parquet(`models/step04_aux/step04_aux_files/omop/concept_ancestor.parquet`);
