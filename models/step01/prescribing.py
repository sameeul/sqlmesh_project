from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_prescribing",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"PRESCRIBINGID": "string", "PATID": "string", "ENCOUNTERID": "string", "RX_PROVIDERID": "string", "RX_ORDER_DATE": "string", "RX_ORDER_TIME": "string", "RX_START_DATE": "string", "RX_END_DATE": "string", "RX_DOSE_ORDERED": "string", "RX_DOSE_ORDERED_UNIT": "string", "RX_QUANTITY": "string", "RX_DOSE_FORM": "string", "RX_REFILLS": "string", "RX_DAYS_SUPPLY": "string", "RX_FREQUENCY": "string", "RX_PRN_FLAG": "string", "RX_ROUTE": "string", "RX_BASIS": "string", "RXNORM_CUI": "string", "RX_SOURCE": "string", "RX_DISPENSE_AS_WRITTEN": "string", "RAW_RX_MED_NAME": "string", "RAW_RX_FREQUENCY": "string", "RAW_RXNORM_CUI": "string", "RAW_RX_QUANTITY": "string", "RAW_RX_NDC": "string", "RAW_RX_DOSE_ORDERED": "string", "RAW_RX_DOSE_ORDERED_UNIT": "string", "RAW_RX_ROUTE": "string", "RAW_RX_REFILLS": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/prescribing.csv")
    )
    validate_required_domain(df, ['prescribingid', 'patid', 'encounterid', 'rx_providerid', 'rx_order_date', 'rx_order_time', 'rx_start_date', 'rx_end_date', 'rx_dose_ordered', 'rx_dose_ordered_unit', 'rx_quantity', 'rx_dose_form', 'rx_refills', 'rx_days_supply', 'rx_frequency', 'rx_prn_flag', 'rx_route', 'rx_basis', 'rxnorm_cui', 'rx_source', 'rx_dispense_as_written', 'raw_rx_med_name', 'raw_rx_frequency', 'raw_rxnorm_cui', 'raw_rx_quantity', 'raw_rx_ndc', 'raw_rx_dose_ordered', 'raw_rx_dose_ordered_unit', 'raw_rx_route', 'raw_rx_refills'], "prescribing")
    return df
