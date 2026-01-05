from sqlmesh import ExecutionContext, model
from util.pcornet.local_schemas import complete_domain_schema_dict, required_domain_schema_dict
from util.pcornet.step02_utils import apply_schema_to_df, validate_required_columns, validate_primary_key


@model(
    name="pcornet.step02_clean_dispensing",
    kind="full",
    dialect="spark",
    tags=["step02"],
    columns={"dispensingid": "string", "patid": "string", "prescribingid": "string", "dispense_date": "date", "ndc": "string", "dispense_source": "string", "dispense_sup": "double", "dispense_amt": "double", "dispense_dose_disp": "double", "dispense_dose_disp_unit": "string", "dispense_route": "string", "raw_ndc": "string", "raw_dispense_dose_disp": "string", "raw_dispense_dose_disp_unit": "string", "raw_dispense_route": "string"},
)
def entrypoint(context: ExecutionContext, **kwargs):
    df = context.table("pcornet.step01_parsed_dispensing")
    required_cols = list(required_domain_schema_dict["dispensing"].keys())
    validate_required_columns(df, required_cols, "dispensing")
    df = apply_schema_to_df(df, complete_domain_schema_dict["dispensing"])
    validate_primary_key(df, "dispensingid", "dispensing")
    return df
