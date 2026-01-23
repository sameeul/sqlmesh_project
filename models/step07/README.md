# Step07 - Pre Clean (Python)

Python models mirroring the legacy step07b pre-clean stage.

Current behavior:
- Pass-through from step06 ID generation.
- For domains with `person_id`, rows are removed if `person_id` appears in `pcornet.step07_pre_clean_removed_person_ids`.

Legacy pre-clean logic (AHRQ xwalk, tribal zips, LOINC removals) is not yet implemented.
