from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_diagnosis",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"diagnosisid": "string", "patid": "string", "encounterid": "string", "enc_type": "string", "admit_date": "date", "providerid": "string", "dx": "string", "dx_type": "string", "dx_date": "date", "dx_source": "string", "dx_origin": "string", "pdx": "string", "dx_poa": "string", "raw_dx": "string", "raw_dx_type": "string", "raw_dx_source": "string", "raw_pdx": "string", "raw_dx_poa": "string", "data_partner_id": "int", "mapped_dx_type": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_diagnosis")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    mapping_df = read_csv(context.spark, "mapping/mapping.csv")
    df = add_mapped_vocab_code_col(df, mapping_df, "DIAGNOSIS", "dx_type", "mapped_dx_type")

    return df
