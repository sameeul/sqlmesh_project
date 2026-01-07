from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_control_map",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"case_patid": "string", "buddy_num": "int", "control_patid": "string", "case_age": "int", "case_sex": "string", "case_race": "string", "case_ethn": "string", "control_age": "int", "control_sex": "string", "control_race": "string", "control_ethn": "string", "data_partner_id": "int"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_control_map")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)


    return df
