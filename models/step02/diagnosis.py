from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_diagnosis",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"diagnosisid": "string", "patid": "string", "encounterid": "string", "enc_type": "string", "admit_date": "date", "providerid": "string", "dx": "string", "dx_type": "string", "dx_date": "date", "dx_source": "string", "dx_origin": "string", "pdx": "string", "dx_poa": "string", "raw_dx": "string", "raw_dx_type": "string", "raw_dx_source": "string", "raw_pdx": "string", "raw_dx_poa": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_diagnosis")
    required_cols = list(required_domain_schema_dict["diagnosis"].keys())
    validate_required_columns(df, required_cols, "diagnosis")
    df = apply_schema_to_df(df, complete_domain_schema_dict["diagnosis"])
    validate_primary_key(df, "diagnosisid", "diagnosis")
    return df
