from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_med_admin",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"medadminid": "string", "patid": "string", "encounterid": "string", "prescribingid": "string", "medadmin_providerid": "string", "medadmin_start_date": "date", "medadmin_start_time": "string", "medadmin_stop_date": "date", "medadmin_stop_time": "string", "medadmin_type": "string", "medadmin_code": "string", "medadmin_dose_admin": "double", "medadmin_dose_admin_unit": "string", "medadmin_route": "string", "medadmin_source": "string", "raw_medadmin_med_name": "string", "raw_medadmin_code": "string", "raw_medadmin_dose_admin": "string", "raw_medadmin_dose_admin_unit": "string", "raw_medadmin_route": "string", "data_partner_id": "int", "mapped_medadmin_type": "string", "MEDADMIN_START_DATETIME": "timestamp", "MEDADMIN_STOP_DATETIME": "timestamp"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_med_admin")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    mapping_df = read_csv(context.spark, "mapping/mapping.csv")
    df = create_datetime_col(df, "medadmin_start_date", "medadmin_start_time", "MEDADMIN_START_DATETIME")
    df = create_datetime_col(df, "medadmin_stop_date", "medadmin_stop_time", "MEDADMIN_STOP_DATETIME")
    df = add_mapped_vocab_code_col(df, mapping_df, "MED_ADMIN", "medadmin_type", "mapped_medadmin_type")

    return df
