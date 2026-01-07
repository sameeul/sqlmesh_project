from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_death",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"PATID": "string", "DEATH_DATE": "string", "DEATH_DATE_IMPUTE": "string", "DEATH_SOURCE": "string", "DEATH_MATCH_CONFIDENCE": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/death.csv")
    )
    validate_required_domain(df, ['patid', 'death_date', 'death_date_impute', 'death_source', 'death_match_confidence'], "death")
    return df
