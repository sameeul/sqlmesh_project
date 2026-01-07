from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_lab_result_cm",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"lab_result_cm_id": "string", "patid": "string", "encounterid": "string", "specimen_source": "string", "lab_loinc": "string", "lab_result_source": "string", "lab_loinc_source": "string", "priority": "string", "result_loc": "string", "lab_px": "string", "lab_px_type": "string", "lab_order_date": "date", "specimen_date": "date", "specimen_time": "string", "result_date": "date", "result_time": "string", "result_qual": "string", "result_snomed": "string", "result_num": "double", "result_modifier": "string", "result_unit": "string", "norm_range_low": "string", "norm_modifier_low": "string", "norm_range_high": "string", "norm_modifier_high": "string", "abn_ind": "string", "raw_lab_name": "string", "raw_lab_code": "string", "raw_panel": "string", "raw_result": "string", "raw_unit": "string", "raw_order_dept": "string", "raw_facility_code": "string", "data_partner_id": "int", "SPECIMEN_DATETIME": "timestamp", "RESULT_DATETIME": "timestamp"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_lab_result_cm")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    from pyspark.sql import functions as F

    df = create_datetime_col(df, "specimen_date", "specimen_time", "SPECIMEN_DATETIME")
    df = create_datetime_col(df, "result_date", "result_time", "RESULT_DATETIME")

    covid_map = read_csv(context.spark, "mapping/covid_test_loinc_map.csv")
    df = df.join(
        covid_map,
        F.upper(df["raw_lab_name"]) == F.upper(covid_map["Covid19LabtestNames"]),
        "left",
    )
    df = df.withColumn("lab_loinc", F.coalesce(df["lab_loinc"], df["AutoLoincCodes"]))
    df = df.drop(
        "Covid19LabtestNames",
        "AutoLoincCodes",
        "LongCommonNames",
        "TestGroup",
        "Notes",
    )

    return df
