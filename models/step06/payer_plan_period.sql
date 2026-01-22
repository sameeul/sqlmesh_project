MODEL (
  name pcornet.step06_id_generation_payer_plan_period,
  kind FULL,
  dialect spark,
  tags ['step06'],
);

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM pcornet.step04_domain_mapping_payer_plan_period m
    LEFT JOIN pcornet.step05_pkey_collision_lookup_payer_plan_period lookup
        ON m.payer_plan_period_id_51_bit = lookup.payer_plan_period_id_51_bit
        AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as payer_plan_period_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(payer_plan_period_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM global_id
INNER JOIN pcornet.step06_id_generation_person p
    ON global_id.site_patid = p.site_patid
