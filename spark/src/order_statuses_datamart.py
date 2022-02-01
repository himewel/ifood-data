import logging
from datetime import datetime
from sys import argv

from delta import DeltaTable
from pyspark.sql.functions import (
    coalesce,
    col,
    collect_list,
    concat,
    date_format,
    lit,
    struct,
    to_date,
)

from utils import get_spark


def get_date_partition():
    date_partition = to_date(
        concat(
            col("year_partition"),
            lit("-"),
            col("month_partition"),
            lit("-"),
            col("day_partition"),
        )
    )
    return date_partition


def transform(start_date, end_date):
    spark = get_spark(app_name="order-statuses-trusted-datamart")

    statuses = (
        spark.read.format("delta")
        .load("s3a://ifood-lake/raw/order_statuses")
        .where(get_date_partition() >= start_date.date())
        .where(get_date_partition() < end_date.date())
    )

    statuses = (
        statuses.select(
            col("order_id"),
            struct(
                col("value").alias("event"),
                col("created_at"),
            ).alias("status"),
        )
        .groupBy("order_id")
        .agg(collect_list("status").alias("status"))
    )
    statuses.printSchema()

    trusted_path = "s3a://ifood-lake/trusted/order_statuses"
    if DeltaTable.isDeltaTable(spark, trusted_path):
        trusted_df = DeltaTable.forPath(spark, trusted_path)
        trusted_df.printSchema()
        _ = (
            trusted_df.alias("trusted")
            .merge(
                source=statuses.alias("orders"),
                condition=col("trusted.order_id") == col("orders.order_id"),
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        _ = statuses.write.format("delta").mode("overwrite").save(trusted_path)


if __name__ == "__main__":
    if len(argv) < 3:
        print("Missing parameters...")
        exit(1)
    else:
        start_date = datetime.strptime(argv[1][:19], "%Y-%m-%dT%H:%M:%S")
        end_date = datetime.strptime(argv[2][:19], "%Y-%m-%dT%H:%M:%S")

    logging.info(f"start_date {start_date}")
    logging.info(f"end_date {end_date}")
    transform(start_date, end_date)
