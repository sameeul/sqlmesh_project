from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_procedures",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"PROCEDURESID": "string", "PATID": "string", "ENCOUNTERID": "string", "ENC_TYPE": "string", "ADMIT_DATE": "string", "PROVIDERID": "string", "PX_DATE": "string", "PX": "string", "PX_TYPE": "string", "PX_SOURCE": "string", "PPX": "string", "RAW_PX": "string", "RAW_PX_TYPE": "string", "RAW_PPX": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/procedures.csv")
    )
    validate_required_domain(df, ['proceduresid', 'patid', 'encounterid', 'enc_type', 'admit_date', 'providerid', 'px_date', 'px', 'px_type', 'px_source', 'ppx', 'raw_px', 'raw_px_type', 'raw_ppx'], "procedures")
    return df
