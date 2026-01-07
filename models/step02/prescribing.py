from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_prescribing",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"prescribingid": "string", "patid": "string", "encounterid": "string", "rx_providerid": "string", "rx_order_date": "date", "rx_order_time": "string", "rx_start_date": "date", "rx_end_date": "date", "rx_dose_ordered": "double", "rx_dose_ordered_unit": "string", "rx_quantity": "double", "rx_dose_form": "string", "rx_refills": "double", "rx_days_supply": "double", "rx_frequency": "string", "rx_prn_flag": "string", "rx_route": "string", "rx_basis": "string", "rxnorm_cui": "string", "rx_source": "string", "rx_dispense_as_written": "string", "raw_rx_med_name": "string", "raw_rx_frequency": "string", "raw_rxnorm_cui": "string", "raw_rx_quantity": "string", "raw_rx_ndc": "string", "raw_rx_dose_ordered": "string", "raw_rx_dose_ordered_unit": "string", "raw_rx_route": "string", "raw_rx_refills": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_prescribing")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["prescribing"]
    validate_required_columns(df, required_cols, "prescribing")
    df = apply_schema_to_df(df, complete_domain_schema_dict["prescribing"])
    validate_primary_key(df, "prescribingid", "prescribing")
    return df
