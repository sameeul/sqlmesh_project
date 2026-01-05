from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_encounter",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"ENCOUNTERID": "string", "PATID": "string", "ADMIT_DATE": "string", "ADMIT_TIME": "string", "DISCHARGE_DATE": "string", "DISCHARGE_TIME": "string", "PROVIDERID": "string", "FACILITY_LOCATION": "string", "ENC_TYPE": "string", "FACILITYID": "string", "DISCHARGE_DISPOSITION": "string", "DISCHARGE_STATUS": "string", "DRG": "string", "DRG_TYPE": "string", "ADMITTING_SOURCE": "string", "PAYER_TYPE_PRIMARY": "string", "PAYER_TYPE_SECONDARY": "string", "FACILITY_TYPE": "string", "RAW_SITEID": "string", "RAW_ENC_TYPE": "string", "RAW_DISCHARGE_DISPOSITION": "string", "RAW_DISCHARGE_STATUS": "string", "RAW_DRG_TYPE": "string", "RAW_ADMITTING_SOURCE": "string", "RAW_FACILITY_TYPE": "string", "RAW_PAYER_TYPE_PRIMARY": "string", "RAW_PAYER_NAME_PRIMARY": "string", "RAW_PAYER_ID_PRIMARY": "string", "RAW_PAYER_TYPE_SECONDARY": "string", "RAW_PAYER_NAME_SECONDARY": "string", "RAW_PAYER_ID_SECONDARY": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/encounter.csv")
    )
    validate_required_domain(df, ['encounterid', 'patid', 'admit_date', 'admit_time', 'discharge_date', 'discharge_time', 'providerid', 'facility_location', 'enc_type', 'facilityid', 'discharge_disposition', 'discharge_status', 'drg', 'drg_type', 'admitting_source', 'payer_type_primary', 'payer_type_secondary', 'facility_type', 'raw_siteid', 'raw_enc_type', 'raw_discharge_disposition', 'raw_discharge_status', 'raw_drg_type', 'raw_admitting_source', 'raw_facility_type', 'raw_payer_type_primary', 'raw_payer_name_primary', 'raw_payer_id_primary', 'raw_payer_type_secondary', 'raw_payer_name_secondary', 'raw_payer_id_secondary'], "encounter")
    return df
