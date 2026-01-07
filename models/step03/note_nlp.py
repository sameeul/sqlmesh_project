from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_note_nlp",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"note_nlp_id": "bigint", "note_id": "bigint", "section_concept_id": "int", "snippet": "string", "offset": "string", "lexical_variant": "string", "note_nlp_concept_id": "int", "note_nlp_source_concept_id": "int", "nlp_system": "string", "nlp_date": "date", "nlp_datetime": "timestamp", "term_exists": "string", "term_temporal": "string", "term_modifiers": "string", "data_partner_id": "int"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_note_nlp")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)


    return df
