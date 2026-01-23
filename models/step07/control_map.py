from sqlmesh import ExecutionContext, model

@model(
    name="pcornet.step07_pre_clean_control_map",
    kind="full",
    dialect="spark",
    tags=["step07"],
)
def entrypoint(context: ExecutionContext, **kwargs):
    return context.spark.table(context.table("pcornet.step06_id_generation_control_map"))
