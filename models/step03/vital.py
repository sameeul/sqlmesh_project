from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_vital",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"vitalid": "string", "patid": "string", "encounterid": "string", "measure_date": "date", "measure_time": "string", "vital_source": "string", "ht": "double", "wt": "double", "diastolic": "double", "systolic": "double", "original_bmi": "double", "bp_position": "string", "smoking": "string", "tobacco": "string", "tobacco_type": "string", "raw_diastolic": "string", "raw_systolic": "string", "raw_bp_position": "string", "raw_smoking": "string", "raw_tobacco": "string", "raw_tobacco_type": "string", "data_partner_id": "int", "MEASURE_DATETIME": "timestamp", "corrected_bp_position": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_vital")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    from pyspark.sql import functions as F

    df = create_datetime_col(df, "measure_date", "measure_time", "MEASURE_DATETIME")
    df = df.withColumn("corrected_bp_position", F.coalesce(df["bp_position"], F.lit("NI")))

    return df
