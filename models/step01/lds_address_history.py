from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_lds_address_history",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"ADDRESSID": "string", "PATID": "string", "ADDRESS_USE": "string", "ADDRESS_TYPE": "string", "ADDRESS_PREFERRED": "string", "ADDRESS_CITY": "string", "ADDRESS_STATE": "string", "ADDRESS_ZIP5": "string", "ADDRESS_ZIP9": "string", "ADDRESS_COUNTY": "string", "ADDRESS_PERIOD_START": "string", "ADDRESS_PERIOD_END": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/lds_address_history.csv")
    )
