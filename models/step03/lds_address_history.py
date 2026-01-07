from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_lds_address_history",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"addressid": "string", "patid": "string", "address_use": "string", "address_type": "string", "address_preferred": "string", "address_city": "string", "address_state": "string", "address_zip5": "string", "address_zip9": "string", "address_county": "string", "address_period_start": "date", "address_period_end": "date", "data_partner_id": "int"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_lds_address_history")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    from pyspark.sql.functions import lit

    if "address_county" not in df.columns:
        df = df.withColumn("address_county", lit(None).cast("string"))

    return df
