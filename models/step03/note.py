from sqlmesh import ExecutionContext, model
from util.pcornet.step03_utils import (
    add_site_id_col,
    apply_site_parsing_logic,
    create_datetime_col,
    add_mapped_vocab_code_col,
    read_csv,
)


@model(
    name="pcornet.step03_prepared_note",
    kind="full",
    dialect="spark",
    tags=["step03"],
    columns={"note_id": "bigint", "person_id": "string", "note_date": "date", "note_datetime": "timestamp", "note_type_concept_id": "int", "note_class_concept_id": "int", "note_title": "string", "note_text": "string", "encoding_concept_id": "int", "language_concept_id": "int", "provider_id": "int", "visit_occurrence_id": "int", "visit_detail_id": "int", "note_source_value": "string", "data_partner_id": "int"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step02_clean_note")
    df = context.spark.table(table_name)

    site_id_df = read_csv(context.spark, "/usr/axle/dev/sqlmesh_project/mapping/site_id.csv")
    df = add_site_id_col(df, site_id_df)
    df = apply_site_parsing_logic(df, site_id_df)


    return df
