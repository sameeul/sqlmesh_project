from sqlmesh import ExecutionContext, model
from util.pcornet.step01_validation import validate_required_domain


@model(
    name="pcornet.step01_parsed_demographic",
    kind="full",
    dialect="spark",
    tags=["step01"],
    columns={"PATID": "string", "BIRTH_DATE": "string", "BIRTH_TIME": "string", "SEX": "string", "SEXUAL_ORIENTATION": "string", "GENDER_IDENTITY": "string", "HISPANIC": "string", "RACE": "string", "BIOBANK_FLAG": "string", "PAT_PREF_LANGUAGE_SPOKEN": "string", "RAW_SEX": "string", "RAW_SEXUAL_ORIENTATION": "string", "RAW_GENDER_IDENTITY": "string", "RAW_HISPANIC": "string", "RAW_RACE": "string", "RAW_PAT_PREF_LANGUAGE_SPOKEN": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = (
        context.spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load("csv_exports/demographic.csv")
    )
    validate_required_domain(df, ['patid', 'birth_date', 'birth_time', 'sex', 'sexual_orientation', 'gender_identity', 'hispanic', 'race', 'biobank_flag', 'pat_pref_language_spoken', 'raw_sex', 'raw_sexual_orientation', 'raw_gender_identity', 'raw_hispanic', 'raw_race', 'raw_pat_pref_language_spoken'], "demographic")
    return df
