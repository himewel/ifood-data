from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name):
    builder = SparkSession.builder.appName(app_name).config(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
