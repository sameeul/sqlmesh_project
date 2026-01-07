from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_encounter",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"encounterid": "string", "patid": "string", "admit_date": "date", "admit_time": "string", "discharge_date": "date", "discharge_time": "string", "providerid": "string", "facility_location": "string", "enc_type": "string", "facilityid": "string", "discharge_disposition": "string", "discharge_status": "string", "drg": "string", "drg_type": "string", "admitting_source": "string", "payer_type_primary": "string", "payer_type_secondary": "string", "facility_type": "string", "raw_siteid": "string", "raw_enc_type": "string", "raw_discharge_disposition": "string", "raw_discharge_status": "string", "raw_drg_type": "string", "raw_admitting_source": "string", "raw_facility_type": "string", "raw_payer_type_primary": "string", "raw_payer_name_primary": "string", "raw_payer_id_primary": "string", "raw_payer_type_secondary": "string", "raw_payer_name_secondary": "string", "raw_payer_id_secondary": "string", "data_partner_id": "int", "ADMIT_DATETIME": "timestamp", "DISCHARGE_DATETIME": "timestamp"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_encounter")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    df = create_datetime_col(df, "admit_date", "admit_time", "ADMIT_DATETIME")
    df = create_datetime_col(df, "discharge_date", "discharge_time", "DISCHARGE_DATETIME")

    return df
