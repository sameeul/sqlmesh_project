MODEL (
  name pcornet.step06_id_generation_person,
  kind FULL,
  dialect spark,
  tags ['step06'],
);

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM pcornet.step04_domain_mapping_person m
    LEFT JOIN pcornet.step05_pkey_collision_lookup_person lookup
        ON m.person_id_51_bit = lookup.person_id_51_bit
        AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as person_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(person_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
),

cte_addr as (
    -- Get most recent address for each patient
    SELECT * FROM (
        SELECT 
            addressid,
            patid,
            address_city,
            address_state,
            address_zip5,
            address_period_start,
            address_period_end,
            data_partner_id,
            CAST(NULL AS string) AS payload,
            Row_Number() Over (Partition By patid Order By COALESCE(address_period_end, CURRENT_DATE()) Desc, address_period_start Desc) as addr_rank
        FROM pcornet.step03_prepared_lds_address_history addr_hist
    )
    WHERE addr_rank = 1
),

pat_to_loc_id_map AS (
    SELECT 
        cte_addr.patid as site_patid,
        loc.*
    FROM cte_addr
        LEFT JOIN pcornet.step06_id_generation_location loc
        ON address_city = loc.city
        AND address_state = loc.state
        AND address_zip5 = loc.zip
)

SELECT
      global_id.*
    -- Join in the final location id after collision resolutions
    , pat_to_loc_id_map.location_id
FROM global_id
LEFT JOIN pat_to_loc_id_map
    ON global_id.site_patid = pat_to_loc_id_map.site_patid
