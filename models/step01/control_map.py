from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_control_map",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"CASE_PATID": "string", "BUDDY_NUM": "string", "CONTROL_PATID": "string", "CASE_AGE": "string", "CASE_SEX": "string", "CASE_RACE": "string", "CASE_ETHN": "string", "CONTROL_AGE": "string", "CONTROL_SEX": "string", "CONTROL_RACE": "string", "CONTROL_ETHN": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/control_map.csv")
    )
