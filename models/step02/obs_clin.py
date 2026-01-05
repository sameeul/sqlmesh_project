from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_versioned, validate_required_columns_any, validate_primary_key


@model(
    name="pcornet.step02_clean_obs_clin",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"obsclinid": "string", "patid": "string", "encounterid": "string", "obsclin_providerid": "string", "obsclin_start_date": "date", "obsclin_start_time": "string", "obsclin_stop_date": "date", "obsclin_stop_time": "string", "obsclin_type": "string", "obsclin_code": "string", "obsclin_result_qual": "string", "obsclin_result_text": "string", "obsclin_result_snomed": "string", "obsclin_result_num": "double", "obsclin_result_modifier": "string", "obsclin_result_unit": "string", "obsclin_source": "string", "obsclin_abn_ind": "string", "raw_obsclin_name": "string", "raw_obsclin_code": "string", "raw_obsclin_type": "string", "raw_obsclin_result": "string", "raw_obsclin_modifier": "string", "raw_obsclin_unit": "string", "obsclin_date": "date", "obsclin_time": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_obs_clin")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["obs_clin"]
    required_cols_v = required_domain_schema_dict["obs_clin_5.0"]
    validate_required_columns_any(df, [required_cols, required_cols_v], "obs_clin")
    df = apply_schema_versioned(
        df,
        complete_domain_schema_dict["obs_clin"],
        complete_domain_schema_dict["obs_clin_5.0"],
        "obsclin_start_date",
    )
    validate_primary_key(df, "obsclinid", "obs_clin")
    return df
