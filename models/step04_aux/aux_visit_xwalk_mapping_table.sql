MODEL (
  name pcornet.aux_visit_xwalk_mapping_table,
  kind FULL,
  dialect spark,
  tags ['step04_aux'],
);

SELECT *
FROM parquet.`step04_aux_files/act_ref/visit_xwalk_mapping_table`;
