from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_immunization",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"immunizationid": "string", "patid": "string", "encounterid": "string", "proceduresid": "string", "vx_providerid": "string", "vx_record_date": "date", "vx_admin_date": "date", "vx_code_type": "string", "vx_code": "string", "vx_status": "string", "vx_status_reason": "string", "vx_source": "string", "vx_dose": "double", "vx_dose_unit": "string", "vx_route": "string", "vx_body_site": "string", "vx_manufacturer": "string", "vx_lot_num": "string", "vx_exp_date": "date", "raw_vx_name": "string", "raw_vx_code": "string", "raw_vx_code_type": "string", "raw_vx_dose": "string", "raw_vx_dose_unit": "string", "raw_vx_route": "string", "raw_vx_body_site": "string", "raw_vx_status": "string", "raw_vx_status_reason": "string", "raw_vx_manufacturer": "string", "data_partner_id": "int", "mapped_vx_code_type": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_immunization")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    mapping_df = read_csv(context.spark, "mapping/mapping.csv")
    df = add_mapped_vocab_code_col(df, mapping_df, "IMMUNIZATION", "vx_code_type", "mapped_vx_code_type")

    return df
