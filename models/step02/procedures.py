from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_procedures",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"proceduresid": "string", "patid": "string", "encounterid": "string", "enc_type": "string", "admit_date": "date", "providerid": "string", "px_date": "date", "px": "string", "px_type": "string", "px_source": "string", "ppx": "string", "raw_px": "string", "raw_px_type": "string", "raw_ppx": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_procedures")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["procedures"]
    validate_required_columns(df, required_cols, "procedures")
    df = apply_schema_to_df(df, complete_domain_schema_dict["procedures"])
    validate_primary_key(df, "proceduresid", "procedures")
    return df
