from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_demographic",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"patid": "string", "birth_date": "date", "birth_time": "string", "sex": "string", "sexual_orientation": "string", "gender_identity": "string", "hispanic": "string", "race": "string", "biobank_flag": "string", "pat_pref_language_spoken": "string", "raw_sex": "string", "raw_sexual_orientation": "string", "raw_gender_identity": "string", "raw_hispanic": "string", "raw_race": "string", "raw_pat_pref_language_spoken": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_demographic")
    required_cols = list(required_domain_schema_dict["demographic"].keys())
    validate_required_columns(df, required_cols, "demographic")
    df = apply_schema_to_df(df, complete_domain_schema_dict["demographic"])
    validate_primary_key(df, "patid", "demographic")
    return df
