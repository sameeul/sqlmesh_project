from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_death_cause",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"PATID": "string", "DEATH_CAUSE": "string", "DEATH_CAUSE_CODE": "string", "DEATH_CAUSE_TYPE": "string", "DEATH_CAUSE_SOURCE": "string", "DEATH_CAUSE_CONFIDENCE": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/death_cause.csv")
    )
