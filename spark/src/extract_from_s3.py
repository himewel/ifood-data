from sys import argv

import yaml
from delta import DeltaTable
from pyspark.sql.functions import (
    col,
    date_format,
    from_json,
    monotonically_increasing_id,
)

from utils import get_spark


def get_schemas():
    with open("/spark/src/schemas.yaml") as stream:
        dtypes = yaml.safe_load(stream)
    return dtypes


def apply_dtypes(df, schema):
    dtypes = get_schemas()[schema]
    for column, dtype in dtypes.items():
        if "\n" in dtype:
            df = df.withColumn(column, from_json(col(column), dtype))
        else:
            df = df.withColumn(column, col(column).cast(dtype))
    return df


def skip_rows(df, rows=1):
    index_col = "index_col"
    df = df.withColumn(index_col, monotonically_increasing_id())
    new_header = df.where(col(index_col) == rows - 1).drop(index_col).first().asDict()
    df = df.select(*[col(old).alias(new) for old, new in new_header.items()], index_col)
    df = df.where(col(index_col) >= rows).drop(index_col)
    return df


def extract_from_s3(source, schema, format, id_columns, partition_column):
    spark = get_spark(app_name=f"{schema}-raw-extraction")
    raw_path = f"s3a://ifood-lake/raw/{schema}"
    df = spark.read.format(format).option("inferSchema", "true").load(source)

    if format == "csv":
        df = skip_rows(df, rows=1)

    df = apply_dtypes(df, schema)
    df = (
        df.withColumn("year_partition", date_format(col(partition_column), "yyyy"))
        .withColumn("month_partition", date_format(col(partition_column), "MM"))
        .withColumn("day_partition", date_format(col(partition_column), "dd"))
    )
    df.printSchema()

    if DeltaTable.isDeltaTable(spark, raw_path):
        raw_df = DeltaTable.forPath(spark, raw_path)
        condition = [f"raw.{column} == landing.{column}" for column in id_columns]
        _ = (
            raw_df.alias("raw")
            .merge(source=df.alias("landing"), condition=" AND ".join(condition))
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        _ = (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("year_partition", "month_partition", "day_partition")
            .save(raw_path)
        )


if __name__ == "__main__":
    if len(argv) < 4:
        print("Missing parameters...")
        exit(1)
    else:
        source = argv[1]
        schema = argv[2]
        format = argv[3]
        id_columns = argv[4].split(",")
        partition_columns = argv[5]

    extract_from_s3(source, schema, format, id_columns, partition_columns)
