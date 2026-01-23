from sqlmesh import ExecutionContext, model

@model(
    name="pcornet.step07_pre_clean_note",
    kind="full",
    dialect="spark",
    tags=["step07"],
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.spark.table(context.table("pcornet.step06_id_generation_note"))
    removed = context.spark.table(context.table("pcornet.step07_pre_clean_removed_person_ids"))
    return df.join(removed, on="person_id", how="left_anti")
