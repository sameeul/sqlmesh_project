from sqlmesh import ExecutionContext, model


@model(
    name="pcornet.step01_parsed_note",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"NOTE_ID": "string", "PERSON_ID": "string", "NOTE_DATE": "string", "NOTE_DATETIME": "string", "NOTE_TYPE_CONCEPT_ID": "string", "NOTE_CLASS_CONCEPT_ID": "string", "NOTE_TITLE": "string", "NOTE_TEXT": "string", "ENCODING_CONCEPT_ID": "string", "LANGUAGE_CONCEPT_ID": "string", "PROVIDER_ID": "string", "VISIT_OCCURRENCE_ID": "string", "VISIT_DETAIL_ID": "string", "NOTE_SOURCE_VALUE": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    return (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(f"{'/usr/axle/dev/sqlmesh_project/csv_exports'}/note.csv")
    )
