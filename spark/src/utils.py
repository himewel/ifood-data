from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, sha1, udf
import pendulum


def get_spark(app_name):
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
            "true",
        )
        .config(
            "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",
            "true",
        )
        .config(
            "spark.databricks.delta.optimizeWrite.enabled",
            "true",
        )
        .config(
            "spark.databricks.delta.autoCompact.enabled",
            "true",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def anonymize_column(df, column):
    df = df.withColumn(column, sha1(md5(col(column).cast("string"))))
    return df


@udf
def convert_datetime(date, timezone):
    """
    Converts a timestamp in UTC time to a timezone and return it in the format
    yyyy-MM-dd HH:mm:ss

    Parameters
        date(TimestampType): timestamp column in UTC timezone
        timezone(StringType): timezone name to convert
    Return
        str: timestamp in the provided timezone name
    """
    utc_date = pendulum.instance(date, "UTC")
    local_date = utc_date.in_timezone(timezone)
    return local_date.to_datetime_string()
