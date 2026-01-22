MODEL (
  name pcornet.step06_id_generation_note_nlp,
  kind FULL,
  dialect spark,
  tags ['step06'],
  physical_properties (foundry_transform_profile = 'EXECUTOR_MEMORY_LARGE'),
);

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM pcornet.step04_domain_mapping_note_nlp m
    LEFT JOIN pcornet.step05_pkey_collision_lookup_note_nlp lookup
        ON m.note_nlp_id_51_bit = lookup.note_nlp_id_51_bit
        AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as note_nlp_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(note_nlp_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final provider, and visit and visit detail ids from the final OMOP domains after collision resolutions
    , note.note_id
FROM global_id
LEFT JOIN pcornet.step06_id_generation_note note
ON global_id.site_note_id = note.site_note_id
