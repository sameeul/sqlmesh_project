MODEL (
  name pcornet.step06_id_generation_visit_detail,
  kind FULL,
  dialect spark,
  tags ['step06'],
);

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM pcornet.step04_domain_mapping_visit_detail m
    LEFT JOIN pcornet.step05_pkey_collision_lookup_visit_detail lookup
        ON m.visit_detail_id_51_bit = lookup.visit_detail_id_51_bit
        AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as visit_detail_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(visit_detail_id_51_bit, 2) + collision_index as local_id
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
INNER JOIN pcornet.step06_id_generation_visit_occurrence v
    ON global_id.site_encounter_id = v.site_encounterid 
    and v.domain_source != 'PROCEDURES'

    --care_site, caresite is not filled in for visit_detail
    --provider_id is not filled in for visit_detail
    --preceding_visit_detail_id from obs_gen is null for visit_detail
