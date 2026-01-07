from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_death_cause",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"patid": "string", "death_cause": "string", "death_cause_code": "string", "death_cause_type": "string", "death_cause_source": "string", "death_cause_confidence": "string", "data_partner_id": "int", "mapped_death_cause_code": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_death_cause")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    mapping_df = read_csv(context.spark, "mapping/mapping.csv")
    df = add_mapped_vocab_code_col(df, mapping_df, "DEATH_CAUSE", "death_cause_code", "mapped_death_cause_code")

    return df
