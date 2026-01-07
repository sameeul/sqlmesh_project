from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step03_unmapped_covid_labs",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"raw_lab_name": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step03_prepared_lab_result_cm")
    df = context.spark.table(table_name)

    from pyspark.sql import functions as F

    return (
        df.where(
            (F.col("lab_loinc").isNull())
            & (F.col("raw_lab_name").rlike("(?i).*sars-cov-2.*"))
        )
        .select("raw_lab_name")
        .distinct()
    )
