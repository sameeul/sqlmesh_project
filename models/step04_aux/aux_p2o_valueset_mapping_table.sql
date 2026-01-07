MODEL (
  name pcornet.aux_p2o_valueset_mapping_table,
  kind FULL,
  dialect spark,
  tags ['step04_aux'],
);

SELECT *
FROM parquet.`step04_aux_files/pcornet/p2o_valueset_mapping_table`;
