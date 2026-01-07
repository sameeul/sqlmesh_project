from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_obs_clin",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"obsclinid": "string", "patid": "string", "encounterid": "string", "obsclin_providerid": "string", "obsclin_start_date": "date", "obsclin_start_time": "string", "obsclin_stop_date": "date", "obsclin_stop_time": "string", "obsclin_type": "string", "obsclin_code": "string", "obsclin_result_qual": "string", "obsclin_result_text": "string", "obsclin_result_snomed": "string", "obsclin_result_num": "double", "obsclin_result_modifier": "string", "obsclin_result_unit": "string", "obsclin_source": "string", "obsclin_abn_ind": "string", "raw_obsclin_name": "string", "raw_obsclin_code": "string", "raw_obsclin_type": "string", "raw_obsclin_result": "string", "raw_obsclin_modifier": "string", "raw_obsclin_unit": "string", "data_partner_id": "int", "mapped_obsclin_type": "string", "OBSCLIN_START_DATETIME": "timestamp", "OBSCLIN_STOP_DATETIME": "timestamp"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_obs_clin")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    from pyspark.sql import functions as F

    # Handle PCORnet 5.0 schema
    if "obsclin_date" in df.columns:
        # Drop existing obsclin_start_date and obsclin_start_time if they exist to avoid conflicts
        cols_to_drop = []
        if "obsclin_start_date" in df.columns and "obsclin_date" in df.columns:
            cols_to_drop.append("obsclin_start_date")
        if "obsclin_start_time" in df.columns and "obsclin_time" in df.columns:
            cols_to_drop.append("obsclin_start_time")
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
            
        df = df.withColumnRenamed("obsclin_date", "obsclin_start_date")
        df = df.withColumnRenamed("obsclin_time", "obsclin_start_time")
        df = df.withColumn("obsclin_stop_date", F.lit(None).cast("date"))
        df = df.withColumn("obsclin_stop_time", F.lit(None).cast("string"))

    df = create_datetime_col(df, "obsclin_start_date", "obsclin_start_time", "OBSCLIN_START_DATETIME")
    df = create_datetime_col(df, "obsclin_stop_date", "obsclin_stop_time", "OBSCLIN_STOP_DATETIME")

    mapping_df = read_csv(context.spark, "mapping/mapping.csv")
    df = add_mapped_vocab_code_col(df, mapping_df, "OBS_CLIN", "obsclin_type", "mapped_obsclin_type")

    return df
