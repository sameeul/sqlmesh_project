from sqlmesh import ExecutionContext, model
from util.pcornet.step05_utils import build_collision_lookup


@model(
    name="pcornet.step05_pkey_collision_lookup_drug_era",
    kind="full",
    dialect="spark",
    tags=["step05"],
    columns={
        "drug_era_id_51_bit": "bigint",
        "hashed_id": "string",
        "collision_bits": "int",
    },
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.spark.table(context.table("pcornet.step04_domain_mapping_drug_era"))
    return build_collision_lookup(df, "drug_era_id_51_bit")
