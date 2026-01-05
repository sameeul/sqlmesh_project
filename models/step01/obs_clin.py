from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_obs_clin",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"OBSCLINID": "string", "PATID": "string", "ENCOUNTERID": "string", "OBSCLIN_PROVIDERID": "string", "OBSCLIN_START_DATE": "string", "OBSCLIN_START_TIME": "string", "OBSCLIN_STOP_DATE": "string", "OBSCLIN_STOP_TIME": "string", "OBSCLIN_TYPE": "string", "OBSCLIN_CODE": "string", "OBSCLIN_RESULT_QUAL": "string", "OBSCLIN_RESULT_TEXT": "string", "OBSCLIN_RESULT_SNOMED": "string", "OBSCLIN_RESULT_NUM": "string", "OBSCLIN_RESULT_MODIFIER": "string", "OBSCLIN_RESULT_UNIT": "string", "OBSCLIN_SOURCE": "string", "OBSCLIN_ABN_IND": "string", "RAW_OBSCLIN_NAME": "string", "RAW_OBSCLIN_CODE": "string", "RAW_OBSCLIN_TYPE": "string", "RAW_OBSCLIN_RESULT": "string", "RAW_OBSCLIN_MODIFIER": "string", "RAW_OBSCLIN_UNIT": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/obs_clin.csv")
    )
