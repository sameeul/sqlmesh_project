from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_demographic",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"patid": "string", "birth_date": "date", "birth_time": "string", "sex": "string", "sexual_orientation": "string", "gender_identity": "string", "hispanic": "string", "race": "string", "biobank_flag": "string", "pat_pref_language_spoken": "string", "raw_sex": "string", "raw_sexual_orientation": "string", "raw_gender_identity": "string", "raw_hispanic": "string", "raw_race": "string", "raw_pat_pref_language_spoken": "string", "data_partner_id": "int", "BIRTH_DATETIME": "timestamp"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_demographic")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)

    df = create_datetime_col(df, "birth_date", "birth_time", "BIRTH_DATETIME")

    return df
