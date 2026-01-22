MODEL (
  name pcornet.step06_id_generation_control_map,
  kind FULL,
  dialect spark,
  tags ['step06'],
);

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM pcornet.step04_domain_mapping_control_map m
    LEFT JOIN pcornet.step05_pkey_collision_lookup_control_map lookup
        ON m.control_map_id_51_bit = lookup.control_map_id_51_bit
        AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as control_map_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(control_map_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
-- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id as case_person_id
    , pp.person_id as control_person_id
FROM global_id
INNER JOIN pcornet.step06_id_generation_person p
    ON global_id.site_case_person_id = p.site_patid
LEFT JOIN pcornet.step06_id_generation_person pp
    ON global_id.site_control_person_id = pp.site_patid
