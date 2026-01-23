from sqlmesh import ExecutionContext, model
import pyspark.sql.types as T

@model(
    name="pcornet.step07_pre_clean_removed_person_ids",
    kind="full",
    dialect="spark",
    tags=["step07"],
    columns={"person_id": "bigint"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    schema = T.StructType([T.StructField("person_id", T.LongType(), True)])
    return context.spark.createDataFrame([], schema)
