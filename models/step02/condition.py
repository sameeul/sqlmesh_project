from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_condition",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"conditionid": "string", "patid": "string", "encounterid": "string", "report_date": "date", "resolve_date": "date", "onset_date": "date", "condition_status": "string", "condition": "string", "condition_type": "string", "condition_source": "string", "raw_condition_status": "string", "raw_condition": "string", "raw_condition_type": "string", "raw_condition_source": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_condition")
    required_cols = list(required_domain_schema_dict["condition"].keys())
    validate_required_columns(df, required_cols, "condition")
    df = apply_schema_to_df(df, complete_domain_schema_dict["condition"])
    validate_primary_key(df, "conditionid", "condition")
    return df
