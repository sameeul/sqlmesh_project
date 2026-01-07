from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_prescribing",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"prescribingid": "string", "patid": "string", "encounterid": "string", "rx_providerid": "string", "rx_order_date": "date", "rx_order_time": "string", "rx_start_date": "date", "rx_end_date": "date", "rx_dose_ordered": "double", "rx_dose_ordered_unit": "string", "rx_quantity": "double", "rx_dose_form": "string", "rx_refills": "double", "rx_days_supply": "double", "rx_frequency": "string", "rx_prn_flag": "string", "rx_route": "string", "rx_basis": "string", "rxnorm_cui": "string", "rx_source": "string", "rx_dispense_as_written": "string", "raw_rx_med_name": "string", "raw_rx_frequency": "string", "raw_rxnorm_cui": "string", "raw_rx_quantity": "string", "raw_rx_ndc": "string", "raw_rx_dose_ordered": "string", "raw_rx_dose_ordered_unit": "string", "raw_rx_route": "string", "raw_rx_refills": "string", "data_partner_id": "int", "RX_ORDER_DATETIME": "timestamp"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_prescribing")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    df = create_datetime_col(df, "rx_order_date", "rx_order_time", "RX_ORDER_DATETIME")

    return df
