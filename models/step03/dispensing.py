from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_dispensing",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"dispensingid": "string", "patid": "string", "prescribingid": "string", "dispense_date": "date", "ndc": "string", "dispense_source": "string", "dispense_sup": "double", "dispense_amt": "double", "dispense_dose_disp": "double", "dispense_dose_disp_unit": "string", "dispense_route": "string", "raw_ndc": "string", "raw_dispense_dose_disp": "string", "raw_dispense_dose_disp_unit": "string", "raw_dispense_route": "string", "data_partner_id": "int"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_dispensing")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)


    return df
