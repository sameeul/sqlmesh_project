from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_note",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"note_id": "bigint", "person_id": "string", "note_date": "date", "note_datetime": "timestamp", "note_type_concept_id": "int", "note_class_concept_id": "int", "note_title": "string", "note_text": "string", "encoding_concept_id": "int", "language_concept_id": "int", "provider_id": "int", "visit_occurrence_id": "int", "visit_detail_id": "int", "note_source_value": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_note")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["note"]
    validate_required_columns(df, required_cols, "note")
    df = apply_schema_to_df(df, complete_domain_schema_dict["note"])
    validate_primary_key(df, "note_id", "note")
    return df
