from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_vital",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"VITALID": "string", "PATID": "string", "ENCOUNTERID": "string", "MEASURE_DATE": "string", "MEASURE_TIME": "string", "VITAL_SOURCE": "string", "HT": "string", "WT": "string", "DIASTOLIC": "string", "SYSTOLIC": "string", "ORIGINAL_BMI": "string", "BP_POSITION": "string", "SMOKING": "string", "TOBACCO": "string", "TOBACCO_TYPE": "string", "RAW_DIASTOLIC": "string", "RAW_SYSTOLIC": "string", "RAW_BP_POSITION": "string", "RAW_SMOKING": "string", "RAW_TOBACCO": "string", "RAW_TOBACCO_TYPE": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/vital.csv")
    )
    validate_required_domain(df, ['vitalid', 'patid', 'encounterid', 'measure_date', 'measure_time', 'vital_source', 'ht', 'wt', 'diastolic', 'systolic', 'original_bmi', 'bp_position', 'smoking', 'tobacco', 'tobacco_type', 'raw_diastolic', 'raw_systolic', 'raw_bp_position', 'raw_smoking', 'raw_tobacco', 'raw_tobacco_type'], "vital")
    return df
