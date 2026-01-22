from sqlmesh import ExecutionContext, model
from util.pcornet.step05_utils import build_collision_lookup


@model(
    name="pcornet.step05_pkey_collision_lookup_payer_plan_period",
    kind="full",
    dialect="spark",
    tags=["step05"],
    columns={
        "payer_plan_period_id_51_bit": "bigint",
        "hashed_id": "string",
        "collision_bits": "int",
    },
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.spark.table(
        context.table("pcornet.step04_domain_mapping_payer_plan_period")
    )
    return build_collision_lookup(df, "payer_plan_period_id_51_bit")
