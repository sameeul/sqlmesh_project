"""Step02 schema application and validation helpers."""

from pyspark.sql import functions as F

from util.pcornet.utils import apply_schema as _apply_schema


def apply_schema_to_df(df, schema_dict):
    """Apply the PCORnet schema to a DataFrame."""
    return _apply_schema(df, schema_dict)


def apply_schema_versioned(df, schema_dict, schema_dict_v, v_column):
    """Apply versioned schema based on the presence of a column."""
    lower_cols = [col.lower() for col in df.columns]
    if v_column.lower() in lower_cols:
        return _apply_schema(df, schema_dict_v)
    return _apply_schema(df, schema_dict)


def validate_required_columns(df, required_columns, domain_name):
    """Validate that all required columns exist in the input DataFrame."""
    expected = {col.lower() for col in required_columns}
    actual = {col.lower() for col in df.columns}
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(
            f"Step02 schema check failed for {domain_name}: missing columns {missing}"
        )


def validate_required_columns_any(df, required_options, domain_name):
    """Validate that at least one required column set exists in the input DataFrame."""
    actual = {col.lower() for col in df.columns}
    for option in required_options:
        expected = {col.lower() for col in option}
        if expected.issubset(actual):
            return
    raise ValueError(
        f"Step02 schema check failed for {domain_name}: no required schema matched"
    )


def validate_primary_key(df, pkey, domain_name):
    """Validate primary key exists, non-null, and unique."""
    if not pkey:
        return
    if pkey not in df.columns:
        raise ValueError(
            f"Step02 primary key check failed for {domain_name}: missing {pkey}"
        )

    total = df.select(pkey).count()
    non_null = df.filter(F.col(pkey).isNotNull()).count()
    if total != non_null:
        raise ValueError(
            f"Step02 primary key check failed for {domain_name}: null {pkey} values"
        )

    distinct = df.select(pkey).distinct().count()
    if distinct != total:
        raise ValueError(
            f"Step02 primary key check failed for {domain_name}: duplicate {pkey} values"
        )
