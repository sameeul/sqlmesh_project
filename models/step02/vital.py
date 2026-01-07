from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_vital",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"vitalid": "string", "patid": "string", "encounterid": "string", "measure_date": "date", "measure_time": "string", "vital_source": "string", "ht": "double", "wt": "double", "diastolic": "double", "systolic": "double", "original_bmi": "double", "bp_position": "string", "smoking": "string", "tobacco": "string", "tobacco_type": "string", "raw_diastolic": "string", "raw_systolic": "string", "raw_bp_position": "string", "raw_smoking": "string", "raw_tobacco": "string", "raw_tobacco_type": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_vital")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["vital"]
    validate_required_columns(df, required_cols, "vital")
    df = apply_schema_to_df(df, complete_domain_schema_dict["vital"])
    validate_primary_key(df, "vitalid", "vital")
    return df
