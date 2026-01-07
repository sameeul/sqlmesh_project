from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_death",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"patid": "string", "death_date": "date", "death_date_impute": "string", "death_source": "string", "death_match_confidence": "string", "data_partner_id": "int"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_death")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)


    return df
