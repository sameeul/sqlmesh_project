from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_note_nlp",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"NOTE_NLP_ID": "string", "NOTE_ID": "string", "SECTION_CONCEPT_ID": "string", "SNIPPET": "string", "OFFSET": "string", "LEXICAL_VARIANT": "string", "NOTE_NLP_CONCEPT_ID": "string", "NOTE_NLP_SOURCE_CONCEPT_ID": "string", "NLP_SYSTEM": "string", "NLP_DATE": "string", "NLP_DATETIME": "string", "TERM_EXISTS": "string", "TERM_TEMPORAL": "string", "TERM_MODIFIERS": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/note_nlp.csv")
    )
