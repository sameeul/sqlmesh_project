"""Step01 validation helpers."""


def validate_required_domain(df, expected_columns, domain_name):
    """
    Validate that required columns exist and the dataset is non-empty.

    Raises ValueError on validation failures.
    """
    expected = {col.lower() for col in expected_columns}
    actual = {col.lower() for col in df.columns}
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(
            f"Step01 schema check failed for {domain_name}: missing columns {missing}"
        )

    # Avoid a full count; limit(1) triggers a minimal scan.
    if df.limit(1).count() == 0:
        raise ValueError(f"Step01 row count check failed for {domain_name}: empty dataset")

    return df
