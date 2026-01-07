from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_condition",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"conditionid": "string", "patid": "string", "encounterid": "string", "report_date": "date", "resolve_date": "date", "onset_date": "date", "condition_status": "string", "condition": "string", "condition_type": "string", "condition_source": "string", "raw_condition_status": "string", "raw_condition": "string", "raw_condition_type": "string", "raw_condition_source": "string", "data_partner_id": "int", "mapped_condition_type": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_condition")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    mapping_df = read_csv(context.spark, "mapping/mapping.csv")
    df = add_mapped_vocab_code_col(df, mapping_df, "CONDITION", "condition_type", "mapped_condition_type")

    return df
