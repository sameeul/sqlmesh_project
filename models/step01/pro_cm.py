from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_pro_cm",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"PRO_CM_ID": "string", "PATID": "string", "ENCOUNTERID": "string", "PRO_DATE": "string", "PRO_TIME": "string", "PRO_TYPE": "string", "PRO_ITEM_NAME": "string", "PRO_ITEM_LOINC": "string", "PRO_RESPONSE_TEXT": "string", "PRO_RESPONSE_NUM": "string", "PRO_METHOD": "string", "PRO_MODE": "string", "PRO_CAT": "string", "PRO_SOURCE": "string", "PRO_ITEM_VERSION": "string", "PRO_MEASURE_NAME": "string", "PRO_MEASURE_SEQ": "string", "PRO_MEASURE_SCORE": "string", "PRO_MEASURE_THETA": "string", "PRO_MEASURE_SCALED_TSCORE": "string", "PRO_MEASURE_STANDARD_ERROR": "string", "PRO_MEASURE_COUNT_SCORED": "string", "PRO_MEASURE_LOINC": "string", "PRO_MEASURE_VERSION": "string", "PRO_ITEM_FULLNAME": "string", "PRO_ITEM_TEXT": "string", "PRO_MEASURE_FULLNAME": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/pro_cm.csv")
    )
