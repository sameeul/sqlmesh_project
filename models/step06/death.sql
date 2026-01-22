MODEL (
  name pcornet.step06_id_generation_death,
  kind FULL,
  dialect spark,
  tags ['step06'],
);

SELECT
      d.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM pcornet.step04_domain_mapping_death d
-- Inner join to remove patients who've been dropped in step04 due to not having a visit/record
INNER JOIN pcornet.step06_id_generation_person p
  ON d.site_patid = p.site_patid
