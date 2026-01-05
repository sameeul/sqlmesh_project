from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_provider",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"PROVIDERID": "string", "PROVIDER_SEX": "string", "PROVIDER_SPECIALTY_PRIMARY": "string", "PROVIDER_NPI": "string", "PROVIDER_NPI_FLAG": "string", "RAW_PROVIDER_SPECIALTY_PRIMARY": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/provider.csv")
    )
