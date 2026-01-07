from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_obs_gen",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"OBSGENID": "string", "PATID": "string", "ENCOUNTERID": "string", "OBSGEN_PROVIDERID": "string", "OBSGEN_START_DATE": "string", "OBSGEN_START_TIME": "string", "OBSGEN_STOP_DATE": "string", "OBSGEN_STOP_TIME": "string", "OBSGEN_TYPE": "string", "OBSGEN_CODE": "string", "OBSGEN_RESULT_QUAL": "string", "OBSGEN_RESULT_TEXT": "string", "OBSGEN_RESULT_NUM": "string", "OBSGEN_RESULT_MODIFIER": "string", "OBSGEN_RESULT_UNIT": "string", "OBSGEN_TABLE_MODIFIED": "string", "OBSGEN_ID_MODIFIED": "string", "OBSGEN_SOURCE": "string", "OBSGEN_ABN_IND": "string", "RAW_OBSGEN_NAME": "string", "RAW_OBSGEN_CODE": "string", "RAW_OBSGEN_TYPE": "string", "RAW_OBSGEN_RESULT": "string", "RAW_OBSGEN_UNIT": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/obs_gen.csv")
    )
