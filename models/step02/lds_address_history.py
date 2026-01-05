from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_lds_address_history",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"addressid": "string", "patid": "string", "address_use": "string", "address_type": "string", "address_preferred": "string", "address_city": "string", "address_state": "string", "address_zip5": "string", "address_zip9": "string", "address_county": "string", "address_period_start": "date", "address_period_end": "date"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_lds_address_history")
    required_cols = list(required_domain_schema_dict["lds_address_history"].keys())
    validate_required_columns(df, required_cols, "lds_address_history")
    df = apply_schema_to_df(df, complete_domain_schema_dict["lds_address_history"])
    validate_primary_key(df, "addressid", "lds_address_history")
    return df
