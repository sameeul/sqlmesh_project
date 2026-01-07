from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_med_admin",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"medadminid": "string", "patid": "string", "encounterid": "string", "prescribingid": "string", "medadmin_providerid": "string", "medadmin_start_date": "date", "medadmin_start_time": "string", "medadmin_stop_date": "date", "medadmin_stop_time": "string", "medadmin_type": "string", "medadmin_code": "string", "medadmin_dose_admin": "double", "medadmin_dose_admin_unit": "string", "medadmin_route": "string", "medadmin_source": "string", "raw_medadmin_med_name": "string", "raw_medadmin_code": "string", "raw_medadmin_dose_admin": "string", "raw_medadmin_dose_admin_unit": "string", "raw_medadmin_route": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_med_admin")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["med_admin"]
    validate_required_columns(df, required_cols, "med_admin")
    df = apply_schema_to_df(df, complete_domain_schema_dict["med_admin"])
    validate_primary_key(df, "medadminid", "med_admin")
    return df
