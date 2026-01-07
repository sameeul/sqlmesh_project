"""Step03 helper utilities for Spark DataFrames."""


def read_csv(spark, path):
    return (
        spark.read.format("csv")
        .options(header="true", inferSchema="false")
        .load(path)
    )


def get_site_id(site_id_df):
    row = site_id_df.select("data_partner_id").head()
    if row is None:
        raise ValueError("site_id.csv is empty")
    return int(row[0])


def add_site_id_col(df, site_id_df):
    from pyspark.sql import functions as F

    site_id = get_site_id(site_id_df)
    df = df.withColumn("data_partner_id", F.lit(site_id).cast("int"))
    return df


def apply_site_parsing_logic(df, _site_id_df):
    # Placeholder for site-specific parsing hooks; currently no-op.
    return df


def create_datetime_col(df, date_col_name, time_col_name, timestamp_col_name):
    from pyspark.sql import functions as F
    
    # Create a combined timestamp string, handling null values properly
    df = df.withColumn(timestamp_col_name, 
                      F.when(F.col(date_col_name).isNotNull(), 
                            F.concat_ws(' ', F.col(date_col_name).cast('string'), 
                                      F.coalesce(F.col(time_col_name), F.lit(''))))
                       .otherwise(F.lit(None).cast('string')))
    
    # Convert to timestamp using to_timestamp which is more robust
    df = df.withColumn(timestamp_col_name, F.to_timestamp(F.col(timestamp_col_name)))
    
    return df


def add_mapped_vocab_code_col(df, mapping_table, domain, source_vocab_col, mapped_col_name):
    from pyspark.sql import functions as F

    mapping_table = (
        mapping_table.where(F.col("cdm_domain") == domain)
        .select("source_vocab_code", "mapped_vocab_code")
    )
    new_df = (
        df.join(
            mapping_table,
            df[source_vocab_col] == mapping_table["source_vocab_code"],
            "left",
        )
        .withColumnRenamed("mapped_vocab_code", mapped_col_name)
        .drop("source_vocab_code")
    )
    return new_df
