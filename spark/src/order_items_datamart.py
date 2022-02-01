import logging
from datetime import datetime
from sys import argv

from delta import DeltaTable
from pyspark.sql.functions import col, concat, explode, lit, to_date

from utils import get_spark


def transform(start_date, end_date):
    spark = get_spark(app_name="order-items-trusted-datamart")

    date_partition = to_date(
        concat(
            col("year_partition"),
            lit("-"),
            col("month_partition"),
            lit("-"),
            col("day_partition"),
        )
    )

    orders = (
        spark.read.format("delta")
        .load("s3a://ifood-lake/raw/orders")
        .where(
            (date_partition >= start_date.date()) & (date_partition < end_date.date())
        )
        .withColumn("items", explode("items"))
        .select(
            "order_id",
            "order_created_at",
            "items.*",
            "year_partition",
            "month_partition",
            "day_partition",
        )
    )

    trusted_path = "s3a://ifood-lake/trusted/order_items"
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
