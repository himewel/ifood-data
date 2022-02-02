import logging
from datetime import datetime
from sys import argv

from delta import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat, date_format, explode, lit, rank, to_date

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
    spark = get_spark(app_name="order-items-trusted-datamart")

    orders = (
        spark.read.format("delta")
        .load("s3a://ifood-lake/raw/orders")
        .where(get_date_partition() >= start_date.date())
        .where(get_date_partition() < end_date.date())
    )

    orders = get_last_order(orders)

    orders = (
        orders.withColumn("item", explode("items"))
        .select(
            "order_id",
            "order_created_at",
            "item",
            col("item.sequence").alias("sequence"),
            date_format(col("order_created_at"), "yyyy").alias("year_partition"),
            date_format(col("order_created_at"), "MM").alias("month_partition"),
            date_format(col("order_created_at"), "dd").alias("day_partition"),
        )
        .drop_duplicates(subset=["order_id", "sequence"])
        .drop("sequence")
    )
    orders.printSchema()

    trusted_path = "s3a://ifood-lake/trusted/order_items"
    if DeltaTable.isDeltaTable(spark, trusted_path):
        trusted_df = DeltaTable.forPath(spark, trusted_path)
        _ = (
            trusted_df.alias("trusted")
            .merge(
                source=orders.alias("orders"),
                condition=(
                    (col("trusted.order_id") == col("orders.order_id"))
                    & (col("trusted.item.sequence") == col("orders.item.sequence"))
                ),
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
