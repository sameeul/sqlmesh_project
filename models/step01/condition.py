from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_condition",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"CONDITIONID": "string", "PATID": "string", "ENCOUNTERID": "string", "REPORT_DATE": "string", "RESOLVE_DATE": "string", "ONSET_DATE": "string", "CONDITION_STATUS": "string", "CONDITION": "string", "CONDITION_TYPE": "string", "CONDITION_SOURCE": "string", "RAW_CONDITION_STATUS": "string", "RAW_CONDITION": "string", "RAW_CONDITION_TYPE": "string", "RAW_CONDITION_SOURCE": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/condition.csv")
    )
    validate_required_domain(df, ['conditionid', 'patid', 'encounterid', 'report_date', 'resolve_date', 'onset_date', 'condition_status', 'condition', 'condition_type', 'condition_source', 'raw_condition_status', 'raw_condition', 'raw_condition_type', 'raw_condition_source'], "condition")
    return df
