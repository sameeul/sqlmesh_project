from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_collision_lookup(df, pk_col):
    """Return lookup rows for 51-bit key collisions based on hashed_id."""
    w = Window.partitionBy(pk_col)
    collisions = (
        df.select(pk_col, "hashed_id")
        .withColumn("pk_count", F.count("*").over(w))
        .filter(F.col("pk_count") > 1)
        .drop("pk_count")
    )
    w2 = Window.partitionBy(pk_col).orderBy("hashed_id")
    return collisions.select(
        F.col(pk_col),
        F.col("hashed_id"),
        (F.row_number().over(w2) - 1).cast("int").alias("collision_bits"),
    )
