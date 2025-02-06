import os

from pyspark.sql import SparkSession


def get_upstream_table(table_name: str, spark: SparkSession):
    host = os.getenv("UPSTREAM_HOST", "0.tcp.in.ngrok.io")
    port = os.getenv("UPSTREAM_PORT", "14185")
    db = os.getenv("UPSTREAM_DATABASE", "helloetl")
    jdbc_url = f'jdbc:postgresql://{host}:{port}/{db}'
    connection_properties = {
        "user": os.getenv("UPSTREAM_USERNAME", "postgres"),
        "password": os.getenv("UPSTREAM_PASSWORD", "abcxyz123"),
        "driver": "org.postgresql.Driver",
    }
    return spark.read.jdbc(
        url=jdbc_url, table=table_name, properties=connection_properties
    )
