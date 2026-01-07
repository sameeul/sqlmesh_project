from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_pro_cm",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"pro_cm_id": "string", "patid": "string", "encounterid": "string", "pro_date": "date", "pro_time": "string", "pro_type": "string", "pro_item_name": "string", "pro_item_loinc": "string", "pro_response_text": "string", "pro_response_num": "double", "pro_method": "string", "pro_mode": "string", "pro_cat": "string", "pro_source": "string", "pro_item_version": "string", "pro_measure_name": "string", "pro_measure_seq": "string", "pro_measure_score": "double", "pro_measure_theta": "double", "pro_measure_scaled_tscore": "double", "pro_measure_standard_error": "double", "pro_measure_count_scored": "double", "pro_measure_loinc": "string", "pro_measure_version": "string", "pro_item_fullname": "string", "pro_item_text": "string", "pro_measure_fullname": "string", "pro_code": "string", "data_partner_id": "int", "PRO_DATETIME": "timestamp"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_pro_cm")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    from pyspark.sql.functions import lit, col, when, array, explode

    df = create_datetime_col(df, 'pro_date', 'pro_time', 'PRO_DATETIME')

    mapping_table = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/mapping.csv")
    df = add_mapped_vocab_code_col(df, mapping_table, "PRO_CM", "pro_item_loinc", "m_pro_item_loinc")
    df = add_mapped_vocab_code_col(df, mapping_table, "PRO_CM", "pro_measure_loinc", "m_pro_measure_loinc")

    return df
