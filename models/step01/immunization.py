from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_immunization",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"IMMUNIZATIONID": "string", "PATID": "string", "ENCOUNTERID": "string", "PROCEDURESID": "string", "VX_PROVIDERID": "string", "VX_RECORD_DATE": "string", "VX_ADMIN_DATE": "string", "VX_CODE_TYPE": "string", "VX_CODE": "string", "VX_STATUS": "string", "VX_STATUS_REASON": "string", "VX_SOURCE": "string", "VX_DOSE": "string", "VX_DOSE_UNIT": "string", "VX_ROUTE": "string", "VX_BODY_SITE": "string", "VX_MANUFACTURER": "string", "VX_LOT_NUM": "string", "VX_EXP_DATE": "string", "RAW_VX_NAME": "string", "RAW_VX_CODE": "string", "RAW_VX_CODE_TYPE": "string", "RAW_VX_DOSE": "string", "RAW_VX_DOSE_UNIT": "string", "RAW_VX_ROUTE": "string", "RAW_VX_BODY_SITE": "string", "RAW_VX_STATUS": "string", "RAW_VX_STATUS_REASON": "string", "RAW_VX_MANUFACTURER": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/immunization.csv")
    )
