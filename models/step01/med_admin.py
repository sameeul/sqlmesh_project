from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_med_admin",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"MEDADMINID": "string", "PATID": "string", "ENCOUNTERID": "string", "PRESCRIBINGID": "string", "MEDADMIN_PROVIDERID": "string", "MEDADMIN_START_DATE": "string", "MEDADMIN_START_TIME": "string", "MEDADMIN_STOP_DATE": "string", "MEDADMIN_STOP_TIME": "string", "MEDADMIN_TYPE": "string", "MEDADMIN_CODE": "string", "MEDADMIN_DOSE_ADMIN": "string", "MEDADMIN_DOSE_ADMIN_UNIT": "string", "MEDADMIN_ROUTE": "string", "MEDADMIN_SOURCE": "string", "RAW_MEDADMIN_MED_NAME": "string", "RAW_MEDADMIN_CODE": "string", "RAW_MEDADMIN_DOSE_ADMIN": "string", "RAW_MEDADMIN_DOSE_ADMIN_UNIT": "string", "RAW_MEDADMIN_ROUTE": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/med_admin.csv")
    )
    validate_required_domain(df, ['medadminid', 'patid', 'encounterid', 'prescribingid', 'medadmin_providerid', 'medadmin_start_date', 'medadmin_start_time', 'medadmin_stop_date', 'medadmin_stop_time', 'medadmin_type', 'medadmin_code', 'medadmin_dose_admin', 'medadmin_dose_admin_unit', 'medadmin_route', 'medadmin_source', 'raw_medadmin_med_name', 'raw_medadmin_code', 'raw_medadmin_dose_admin', 'raw_medadmin_dose_admin_unit', 'raw_medadmin_route'], "med_admin")
    return df
