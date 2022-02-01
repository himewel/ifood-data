import logging
from datetime import datetime
from sys import argv

from delta import DeltaTable
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    date_format,
    lit,
    struct,
    to_date,
)

from utils import get_spark


def transform(start_date, end_date):
    spark = get_spark(app_name="order-statuses-trusted-datamart")

    date_partition = to_date(
        concat(
            col("year_partition"),
            lit("-"),
            col("month_partition"),
            lit("-"),
            col("day_partition"),
        )
    )
    statuses = (
        spark.read.format("delta")
        .load("s3a://ifood-lake/raw/order_statuses")
        .where(
            (date_partition >= start_date.date()) & (date_partition < end_date.date())
        )
    )

    values = ["concluded", "registered", "cacelled", "placed"]
    orders = statuses.select("order_id").distinct().alias("orders")
    for status in values:
        status_value = statuses.where(col("value") == status.upper()).select(
            "order_id", "created_at"
        )
        orders = (
            orders.join(
                other=status_value.alias(status),
                on=col("orders.order_id") == col(f"{status}.order_id"),
                how="left",
            )
            .withColumn(f"{status}_timestamp", col(f"{status}.created_at"))
            .drop(f"{status}.*")
        )

    timestamp_columns = [f"{status}_timestamp" for status in values]
    date_partition = coalesce(*timestamp_columns).alias("date_partition")
    orders = (
        orders.select("orders.order_id", *timestamp_columns)
        .withColumn("year_partition", date_format(date_partition, "yyyy"))
        .withColumn("month_partition", date_format(date_partition, "MM"))
        .withColumn("day_partition", date_format(date_partition, "dd"))
    )

    trusted_path = "s3a://ifood-lake/trusted/order_statuses"
    if DeltaTable.isDeltaTable(spark, trusted_path):
        trusted_df = DeltaTable.forPath(spark, trusted_path)
        for status in values:
            status_orders = orders.where(col(f"{status}_timestamp").isNotNull())
            _ = (
                trusted_df.alias("trusted")
                .merge(
                    source=status_orders.alias("orders"),
                    condition=col("trusted.order_id") == col("orders.order_id"),
                )
                .whenMatchedUpdate(
                    set={f"{status}_timestamp": f"orders.{status}_timestamp"}
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
    else:
        _ = (
            orders.write.format("delta")
            .partitionBy("year_partition", "month_partition", "day_partition")
            .mode("overwrite")
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
