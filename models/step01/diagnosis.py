from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_diagnosis",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"DIAGNOSISID": "string", "PATID": "string", "ENCOUNTERID": "string", "ENC_TYPE": "string", "ADMIT_DATE": "string", "PROVIDERID": "string", "DX": "string", "DX_TYPE": "string", "DX_DATE": "string", "DX_SOURCE": "string", "DX_ORIGIN": "string", "PDX": "string", "DX_POA": "string", "RAW_DX": "string", "RAW_DX_TYPE": "string", "RAW_DX_SOURCE": "string", "RAW_PDX": "string", "RAW_DX_POA": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/diagnosis.csv")
    )
    validate_required_domain(df, ['diagnosisid', 'patid', 'encounterid', 'enc_type', 'admit_date', 'providerid', 'dx', 'dx_type', 'dx_date', 'dx_source', 'dx_origin', 'pdx', 'dx_poa', 'raw_dx', 'raw_dx_type', 'raw_dx_source', 'raw_pdx', 'raw_dx_poa'], "diagnosis")
    return df
