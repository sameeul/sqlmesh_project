from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_encounter",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"encounterid": "string", "patid": "string", "admit_date": "date", "admit_time": "string", "discharge_date": "date", "discharge_time": "string", "providerid": "string", "facility_location": "string", "enc_type": "string", "facilityid": "string", "discharge_disposition": "string", "discharge_status": "string", "drg": "string", "drg_type": "string", "admitting_source": "string", "payer_type_primary": "string", "payer_type_secondary": "string", "facility_type": "string", "raw_siteid": "string", "raw_enc_type": "string", "raw_discharge_disposition": "string", "raw_discharge_status": "string", "raw_drg_type": "string", "raw_admitting_source": "string", "raw_facility_type": "string", "raw_payer_type_primary": "string", "raw_payer_name_primary": "string", "raw_payer_id_primary": "string", "raw_payer_type_secondary": "string", "raw_payer_name_secondary": "string", "raw_payer_id_secondary": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_encounter")
    required_cols = list(required_domain_schema_dict["encounter"].keys())
    validate_required_columns(df, required_cols, "encounter")
    df = apply_schema_to_df(df, complete_domain_schema_dict["encounter"])
    validate_primary_key(df, "encounterid", "encounter")
    return df
