from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_dispensing",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"DISPENSINGID": "string", "PATID": "string", "PRESCRIBINGID": "string", "DISPENSE_DATE": "string", "NDC": "string", "DISPENSE_SOURCE": "string", "DISPENSE_SUP": "string", "DISPENSE_AMT": "string", "DISPENSE_DOSE_DISP": "string", "DISPENSE_DOSE_DISP_UNIT": "string", "DISPENSE_ROUTE": "string", "RAW_NDC": "string", "RAW_DISPENSE_DOSE_DISP": "string", "RAW_DISPENSE_DOSE_DISP_UNIT": "string", "RAW_DISPENSE_ROUTE": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/dispensing.csv")
    )
