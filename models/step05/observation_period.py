from sqlmesh import ExecutionContext, model
from util.pcornet.step05_utils import build_collision_lookup


@model(
    name="pcornet.step05_pkey_collision_lookup_observation_period",
    kind="full",
    dialect="spark",
    tags=["step05"],
    columns={"observation_period_id_51_bit": "bigint", "hashed_id": "string", "collision_bits": "int"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.spark.table(context.table("pcornet.step04_domain_mapping_observation_period"))
    return build_collision_lookup(df, "observation_period_id_51_bit")
