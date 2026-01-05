from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_death_cause",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"patid": "string", "death_cause": "string", "death_cause_code": "string", "death_cause_type": "string", "death_cause_source": "string", "death_cause_confidence": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_death_cause")
    required_cols = list(required_domain_schema_dict["death_cause"].keys())
    validate_required_columns(df, required_cols, "death_cause")
    df = apply_schema_to_df(df, complete_domain_schema_dict["death_cause"])
    validate_primary_key(df, None, "death_cause")
    return df
