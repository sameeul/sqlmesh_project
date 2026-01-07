from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_procedures",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"proceduresid": "string", "patid": "string", "encounterid": "string", "enc_type": "string", "admit_date": "date", "providerid": "string", "px_date": "date", "px": "string", "px_type": "string", "px_source": "string", "ppx": "string", "raw_px": "string", "raw_px_type": "string", "raw_ppx": "string", "data_partner_id": "int", "mapped_px_type": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_procedures")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    mapping_df = read_csv(context.spark, "mapping/mapping.csv")
    df = add_mapped_vocab_code_col(df, mapping_df, "PROCEDURES", "px_type", "mapped_px_type")

    return df
