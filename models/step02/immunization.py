from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_immunization",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"immunizationid": "string", "patid": "string", "encounterid": "string", "proceduresid": "string", "vx_providerid": "string", "vx_record_date": "date", "vx_admin_date": "date", "vx_code_type": "string", "vx_code": "string", "vx_status": "string", "vx_status_reason": "string", "vx_source": "string", "vx_dose": "double", "vx_dose_unit": "string", "vx_route": "string", "vx_body_site": "string", "vx_manufacturer": "string", "vx_lot_num": "string", "vx_exp_date": "date", "raw_vx_name": "string", "raw_vx_code": "string", "raw_vx_code_type": "string", "raw_vx_dose": "string", "raw_vx_dose_unit": "string", "raw_vx_route": "string", "raw_vx_body_site": "string", "raw_vx_status": "string", "raw_vx_status_reason": "string", "raw_vx_manufacturer": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_immunization")
    required_cols = list(required_domain_schema_dict["immunization"].keys())
    validate_required_columns(df, required_cols, "immunization")
    df = apply_schema_to_df(df, complete_domain_schema_dict["immunization"])
    validate_primary_key(df, "immunizationid", "immunization")
    return df
