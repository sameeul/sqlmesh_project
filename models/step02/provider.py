from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_provider",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"providerid": "string", "provider_sex": "string", "provider_specialty_primary": "string", "provider_npi": "double", "provider_npi_flag": "string", "raw_provider_specialty_primary": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_provider")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["provider"]
    validate_required_columns(df, required_cols, "provider")
    df = apply_schema_to_df(df, complete_domain_schema_dict["provider"])
    validate_primary_key(df, "providerid", "provider")
    return df
