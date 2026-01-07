from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_obs_gen",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"obsgenid": "string", "patid": "string", "encounterid": "string", "obsgen_providerid": "string", "obsgen_start_date": "date", "obsgen_start_time": "string", "obsgen_stop_date": "date", "obsgen_stop_time": "string", "obsgen_type": "string", "obsgen_code": "string", "obsgen_result_qual": "string", "obsgen_result_text": "string", "obsgen_result_num": "double", "obsgen_result_modifier": "string", "obsgen_result_unit": "string", "obsgen_table_modified": "string", "obsgen_id_modified": "string", "obsgen_source": "string", "obsgen_abn_ind": "string", "raw_obsgen_name": "string", "raw_obsgen_code": "string", "raw_obsgen_type": "string", "raw_obsgen_result": "string", "raw_obsgen_unit": "string", "data_partner_id": "int", "mapped_obsgen_type": "string", "OBSGEN_START_DATETIME": "timestamp", "OBSGEN_STOP_DATETIME": "timestamp"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_obs_gen")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    from pyspark.sql import functions as F

    # Handle PCORnet 5.0 schema
    if "obsgen_date" in df.columns:
        # Drop existing obsgen_start_date and obsgen_start_time if they exist to avoid conflicts
        cols_to_drop = []
        if "obsgen_start_date" in df.columns and "obsgen_date" in df.columns:
            cols_to_drop.append("obsgen_start_date")
        if "obsgen_start_time" in df.columns and "obsgen_time" in df.columns:
            cols_to_drop.append("obsgen_start_time")
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
            
        df = df.withColumnRenamed("obsgen_date", "obsgen_start_date")
        df = df.withColumnRenamed("obsgen_time", "obsgen_start_time")
        df = df.withColumn("obsgen_stop_date", F.lit(None).cast("date"))
        df = df.withColumn("obsgen_stop_time", F.lit(None).cast("string"))

    df = create_datetime_col(df, "obsgen_start_date", "obsgen_start_time", "OBSGEN_START_DATETIME")
    df = create_datetime_col(df, "obsgen_stop_date", "obsgen_stop_time", "OBSGEN_STOP_DATETIME")

    mapping_df = read_csv(context.spark, "mapping/mapping.csv")
    df = add_mapped_vocab_code_col(df, mapping_df, "OBS_GEN", "obsgen_type", "mapped_obsgen_type")

    return df
