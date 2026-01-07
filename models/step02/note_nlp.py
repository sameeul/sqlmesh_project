from sqlmesh import ExecutionContext, model
from util.pcornet.step02_columns import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_note_nlp",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"note_nlp_id": "bigint", "note_id": "bigint", "section_concept_id": "int", "snippet": "string", "offset": "string", "lexical_variant": "string", "note_nlp_concept_id": "int", "note_nlp_source_concept_id": "int", "nlp_system": "string", "nlp_date": "date", "nlp_datetime": "timestamp", "term_exists": "string", "term_temporal": "string", "term_modifiers": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    table_name = context.table("pcornet.step01_parsed_note_nlp")
    df = context.spark.table(table_name)
    required_cols = required_domain_schema_dict["note_nlp"]
    validate_required_columns(df, required_cols, "note_nlp")
    df = apply_schema_to_df(df, complete_domain_schema_dict["note_nlp"])
    validate_primary_key(df, "note_nlp_id", "note_nlp")
    return df
