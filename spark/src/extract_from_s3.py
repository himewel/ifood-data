import logging
from datetime import datetime
from sys import argv

import yaml
from delta import DeltaTable
from pyspark.sql.functions import (
    col,
    concat,
    date_format,
    from_json,
    lit,
    monotonically_increasing_id,
    to_date,
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


def read_data(spark, format, source, schema, partition_column, start_date, end_date):
    df = spark.read.format(format).option("inferSchema", "true").load(source)

    if format == "csv":
        df = skip_rows(df, rows=1)

    df = (
        apply_dtypes(df, schema)
        .where(
            (col(partition_column) >= start_date) & (col(partition_column) < end_date)
        )
        .withColumn("year_partition", date_format(col(partition_column), "yyyy"))
        .withColumn("month_partition", date_format(col(partition_column), "MM"))
        .withColumn("day_partition", date_format(col(partition_column), "dd"))
    )
    return df


def test_extraction(
    source,
    schema,
    format,
    id_columns,
    partition_column,
    start_date,
    end_date,
):
    raw_path = f"s3a://ifood-lake/raw/{schema}"
    spark = get_spark(app_name=f"{schema}-raw-test")
    df = read_data(
        spark, format, source, schema, partition_column, start_date, end_date
    )

    distinct_ids = df.select(*id_columns).distinct()
    date_partition = to_date(
        concat(
            col("year_partition"),
            lit("-"),
            col("month_partition"),
            lit("-"),
            col("day_partition"),
        )
    )
    raw_df = (
        spark.read.format("delta")
        .load(raw_path)
        .where(
            (date_partition >= start_date.date()) & (date_partition < end_date.date())
        )
    )
    null_ids = (
        distinct_ids.alias("ids")
        .join(
            other=raw_df.alias("raw"),
            on=[col(f"ids.{id}") == col(f"raw.{id}") for id in id_columns],
            how="leftanti",
        )
        .count()
    )
    if null_ids > 0:
        raise Exception("Some rows were not ingested in raw layer")


def extract_from_s3(
    source,
    schema,
    format,
    id_columns,
    partition_column,
    start_date,
    end_date,
):
    spark = get_spark(app_name=f"{schema}-raw-extraction")
    df = read_data(
        spark, format, source, schema, partition_column, start_date, end_date
    )

    df.printSchema()
    if df.count() == 0:
        logging.info("Dataframe is empty")
        return

    df = df.drop_duplicates(id_columns + [partition_column])

    raw_path = f"s3a://ifood-lake/raw/{schema}"
    if DeltaTable.isDeltaTable(spark, raw_path):
        raw_df = DeltaTable.forPath(spark, raw_path)
        condition = [f"raw.{column} = landing.{column}" for column in id_columns]
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
    if len(argv) < 9:
        print("Missing parameters...")
        exit(1)
    else:
        source = argv[1]
        schema = argv[2]
        format = argv[3]
        id_columns = argv[4].split(",")
        partition_columns = argv[5]
        start_date = datetime.strptime(argv[6][:19], "%Y-%m-%dT%H:%M:%S")
        end_date = datetime.strptime(argv[7][:19], "%Y-%m-%dT%H:%M:%S")
        mode = argv[8]

    logging.info(f"source {source}")
    logging.info(f"schema {schema}")
    logging.info(f"format {format}")
    logging.info(f"id_columns {id_columns}")
    logging.info(f"partition_columns {partition_columns}")
    logging.info(f"start_date {start_date}")
    logging.info(f"end_date {end_date}")
    logging.info(f"mode {mode}")

    if mode == "test":
        test_extraction(
            source,
            schema,
            format,
            id_columns,
            partition_columns,
            start_date,
            end_date,
        )
    elif mode == "run":
        extract_from_s3(
            source,
            schema,
            format,
            id_columns,
            partition_columns,
            start_date,
            end_date,
        )
