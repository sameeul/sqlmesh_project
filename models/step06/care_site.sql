MODEL (
  name pcornet.step06_id_generation_care_site,
  kind FULL,
  dialect spark,
  tags ['step06'],
);

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM pcornet.step04_domain_mapping_care_site m
    LEFT JOIN pcornet.step05_pkey_collision_lookup_care_site lookup
        ON m.care_site_id_51_bit = lookup.care_site_id_51_bit
        AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as care_site_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(care_site_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    , CAST(null as long) as location_id
FROM global_id
