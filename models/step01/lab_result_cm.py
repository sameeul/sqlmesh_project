from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_lab_result_cm",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"LAB_RESULT_CM_ID": "string", "PATID": "string", "ENCOUNTERID": "string", "SPECIMEN_SOURCE": "string", "LAB_LOINC": "string", "LAB_RESULT_SOURCE": "string", "LAB_LOINC_SOURCE": "string", "PRIORITY": "string", "RESULT_LOC": "string", "LAB_PX": "string", "LAB_PX_TYPE": "string", "LAB_ORDER_DATE": "string", "SPECIMEN_DATE": "string", "SPECIMEN_TIME": "string", "RESULT_DATE": "string", "RESULT_TIME": "string", "RESULT_QUAL": "string", "RESULT_SNOMED": "string", "RESULT_NUM": "string", "RESULT_MODIFIER": "string", "RESULT_UNIT": "string", "NORM_RANGE_LOW": "string", "NORM_MODIFIER_LOW": "string", "NORM_RANGE_HIGH": "string", "NORM_MODIFIER_HIGH": "string", "ABN_IND": "string", "RAW_LAB_NAME": "string", "RAW_LAB_CODE": "string", "RAW_PANEL": "string", "RAW_RESULT": "string", "RAW_UNIT": "string", "RAW_ORDER_DEPT": "string", "RAW_FACILITY_CODE": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/lab_result_cm.csv")
    )
    validate_required_domain(df, ['lab_result_cm_id', 'patid', 'encounterid', 'specimen_source', 'lab_loinc', 'lab_result_source', 'lab_loinc_source', 'priority', 'result_loc', 'lab_px', 'lab_px_type', 'lab_order_date', 'specimen_date', 'specimen_time', 'result_date', 'result_time', 'result_qual', 'result_snomed', 'result_num', 'result_modifier', 'result_unit', 'norm_range_low', 'norm_modifier_low', 'norm_range_high', 'norm_modifier_high', 'abn_ind', 'raw_lab_name', 'raw_lab_code', 'raw_panel', 'raw_result', 'raw_unit', 'raw_order_dept', 'raw_facility_code'], "lab_result_cm")
    return df
