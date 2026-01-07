from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_control_map",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"case_patid": "string", "buddy_num": "int", "control_patid": "string", "case_age": "int", "case_sex": "string", "case_race": "string", "case_ethn": "string", "control_age": "int", "control_sex": "string", "control_race": "string", "control_ethn": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_control_map")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["control_map"]
    validate_required_columns(df, required_cols, "control_map")
    df = apply_schema_to_df(df, complete_domain_schema_dict["control_map"])
    validate_primary_key(df, None, "control_map")
    return df
