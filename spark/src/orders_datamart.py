import logging
from datetime import datetime
from sys import argv

from delta import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    concat,
    date_format,
    lit,
    rank,
    struct,
    to_date,
    to_timestamp,
)

from utils import get_spark, anonymize_column, convert_datetime

to_anonymize = [
    "cpf",
    "customer_phone_number",
    "delivery_address_latitude",
    "delivery_address_longitude",
]


def get_last_statuses(df):
    partition = Window.partitionBy(col("order_id")).orderBy(col("created_at").desc())
    df = (
        df.withColumn("rank_value", rank().over(partition))
        .where(col("rank_value") == 1)
        .drop("rank_value")
    )
    return df


def get_last_order(df):
    partition = Window.partitionBy(col("order_id")).orderBy(
        col("order_created_at").desc()
    )
    df = (
        df.withColumn("rank_value", rank().over(partition))
        .where(col("rank_value") == 1)
        .drop("rank_value")
    )
    return df


def transform(start_date, end_date):
    spark = get_spark(app_name="orders-trusted-datamart")
    s3_path = "s3a://ifood-lake"

    date_partition = to_date(
        concat(
            col("year_partition"),
            lit("-"),
            col("month_partition"),
            lit("-"),
            col("day_partition"),
        )
    )

    orders = spark.read.format("delta").load(f"{s3_path}/raw/orders")
    consumers = spark.read.format("delta").load(f"{s3_path}/raw/consumers")
    restaurants = spark.read.format("delta").load(f"{s3_path}/raw/restaurants")
    order_statuses = (
        spark.read.format("delta")
        .load(f"{s3_path}/raw/order_statuses")
        .where(
            (date_partition >= start_date.date()) & (date_partition < end_date.date())
        )
    )

    order_statuses = get_last_statuses(order_statuses)
    orders = get_last_order(orders)

    orders = (
        orders.alias("orders")
        .join(
            other=order_statuses.alias("status"),
            on=[col("orders.order_id") == col("status.order_id")],
            how="left",
        )
        .join(
            other=consumers.alias("consumers"),
            on=[col("orders.customer_id") == col("consumers.customer_id")],
            how="left",
        )
        .join(
            other=restaurants.alias("restaurants"),
            on=[col("orders.merchant_id") == col("restaurants.id")],
            how="left",
        )
    )

    for column in to_anonymize:
        orders = anonymize_column(orders, column)

    orders = orders.select(
        "orders.*",
        struct("consumers.*").alias("consumers"),
        struct("restaurants.*").alias("restaurants"),
        struct("status.*").alias("status"),
    )

    orders.printSchema()

    orders = (
        orders.withColumn(
            "order_created_at",
            to_timestamp(
                convert_datetime(
                    col("order_created_at"),
                    col("merchant_timezone"),
                )
            ),
        )
        .withColumn("year_partition", date_format(col("order_created_at"), "yyyy"))
        .withColumn("month_partition", date_format(col("order_created_at"), "MM"))
        .withColumn("day_partition", date_format(col("order_created_at"), "dd"))
    )

    trusted_path = f"{s3_path}/trusted/orders"
    if DeltaTable.isDeltaTable(spark, trusted_path):
        trusted_df = DeltaTable.forPath(spark, trusted_path)
        _ = (
            trusted_df.alias("trusted")
            .merge(
                source=orders.alias("orders"),
                condition=col("trusted.order_id") == col("orders.order_id"),
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        _ = (
            orders.write.format("delta")
            .mode("overwrite")
            .partitionBy("year_partition", "month_partition", "day_partition")
            .save(trusted_path)
        )


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
