from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_lab_result_cm",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"lab_result_cm_id": "string", "patid": "string", "encounterid": "string", "specimen_source": "string", "lab_loinc": "string", "lab_result_source": "string", "lab_loinc_source": "string", "priority": "string", "result_loc": "string", "lab_px": "string", "lab_px_type": "string", "lab_order_date": "date", "specimen_date": "date", "specimen_time": "string", "result_date": "date", "result_time": "string", "result_qual": "string", "result_snomed": "string", "result_num": "double", "result_modifier": "string", "result_unit": "string", "norm_range_low": "string", "norm_modifier_low": "string", "norm_range_high": "string", "norm_modifier_high": "string", "abn_ind": "string", "raw_lab_name": "string", "raw_lab_code": "string", "raw_panel": "string", "raw_result": "string", "raw_unit": "string", "raw_order_dept": "string", "raw_facility_code": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_lab_result_cm")
    required_cols = list(required_domain_schema_dict["lab_result_cm"].keys())
    validate_required_columns(df, required_cols, "lab_result_cm")
    df = apply_schema_to_df(df, complete_domain_schema_dict["lab_result_cm"])
    validate_primary_key(df, "lab_result_cm_id", "lab_result_cm")
    return df
