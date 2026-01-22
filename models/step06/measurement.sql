MODEL (
  name pcornet.step06_id_generation_measurement,
  kind FULL,
  dialect spark,
  tags ['step06'],
  physical_properties (foundry_transform_profile = 'EXECUTOR_MEMORY_LARGE'),
);

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM pcornet.step04_domain_mapping_measurement m
    LEFT JOIN pcornet.step05_pkey_collision_lookup_measurement lookup
        ON m.measurement_id_51_bit = lookup.measurement_id_51_bit
        AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as measurement_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(measurement_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
    , v.visit_occurrence_id
FROM global_id
INNER JOIN pcornet.step06_id_generation_person p
    ON global_id.site_patid = p.site_patid
LEFT JOIN pcornet.step06_id_generation_visit_occurrence v
    ON global_id.site_encounterid = v.site_encounterid
    and v.domain_source != 'PROCEDURES'
