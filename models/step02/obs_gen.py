from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_versioned, validate_required_columns_any, validate_primary_key


@model(
    name="pcornet.step02_clean_obs_gen",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"obsgenid": "string", "patid": "string", "encounterid": "string", "obsgen_providerid": "string", "obsgen_start_date": "date", "obsgen_start_time": "string", "obsgen_stop_date": "date", "obsgen_stop_time": "string", "obsgen_type": "string", "obsgen_code": "string", "obsgen_result_qual": "string", "obsgen_result_text": "string", "obsgen_result_num": "double", "obsgen_result_modifier": "string", "obsgen_result_unit": "string", "obsgen_table_modified": "string", "obsgen_id_modified": "string", "obsgen_source": "string", "obsgen_abn_ind": "string", "raw_obsgen_name": "string", "raw_obsgen_code": "string", "raw_obsgen_type": "string", "raw_obsgen_result": "string", "raw_obsgen_unit": "string", "obsgen_date": "date", "obsgen_time": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_obs_gen")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["obs_gen"]
    required_cols_v = required_domain_schema_dict["obs_gen_5.0"]
    validate_required_columns_any(df, [required_cols, required_cols_v], "obs_gen")
    df = apply_schema_versioned(
        df,
        complete_domain_schema_dict["obs_gen"],
        complete_domain_schema_dict["obs_gen_5.0"],
        "obsgen_start_date",
    )
    validate_primary_key(df, "obsgenid", "obs_gen")
    return df
