from sqlmesh import ExecutionContext, model

@model(
    name="pcornet.step08_clean_visit_occurrence",
    kind="full",
    dialect="spark",
    tags=["step08"],
)
def entrypoint(context: ExecutionContext, **kwargs):
    return context.spark.table(context.table("pcornet.step07_pre_clean_visit_occurrence"))
