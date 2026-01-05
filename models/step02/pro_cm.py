from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_versioned, validate_required_columns_any, validate_primary_key


@model(
    name="pcornet.step02_clean_pro_cm",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"pro_cm_id": "string", "patid": "string", "encounterid": "string", "pro_date": "date", "pro_time": "string", "pro_type": "string", "pro_item_name": "string", "pro_item_loinc": "string", "pro_response_text": "string", "pro_response_num": "double", "pro_method": "string", "pro_mode": "string", "pro_cat": "string", "pro_source": "string", "pro_item_version": "string", "pro_measure_name": "string", "pro_measure_seq": "string", "pro_measure_score": "double", "pro_measure_theta": "double", "pro_measure_scaled_tscore": "double", "pro_measure_standard_error": "double", "pro_measure_count_scored": "double", "pro_measure_loinc": "string", "pro_measure_version": "string", "pro_item_fullname": "string", "pro_item_text": "string", "pro_measure_fullname": "string", "pro_code": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_pro_cm")
    required_cols = list(required_domain_schema_dict["pro_cm"].keys())
    required_cols_v = list(required_domain_schema_dict["pro_cm_v7"].keys())
    validate_required_columns_any(df, [required_cols, required_cols_v], "pro_cm")
    df = apply_schema_versioned(
        df,
        complete_domain_schema_dict["pro_cm"],
        complete_domain_schema_dict["pro_cm_v7"],
        "pro_code",
    )
    validate_primary_key(df, "pro_cm_id", "pro_cm")
    return df
