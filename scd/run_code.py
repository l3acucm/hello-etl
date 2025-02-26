from pyspark.sql import SparkSession

from scd.etl.gold import SCDUserGoldETL
from scd.etl.interface.scd_user_report import create_scd_users_report_view


def run_code(spark):
    scd_users_metrics = SCDUserGoldETL(spark=spark)
    # scd_users_metrics = SCDUserGoldETL(spark=spark)
    scd_users_metrics.run()
    create_scd_users_report_view(scd_users_metrics.read().curr_data)
    print("=================================")
    print("SCD2 Users Report")
    print("=================================")
    spark.sql("select * from global_temp.scd_users_report").show()


if __name__ == "__main__":
    # Create a spark session
    spark = (
        SparkSession.builder.appName("SCD2 from scratch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", "")
        .config("spark.hadoop.fs.s3a.secret.key", "")
        #.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "eu-central-1")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark)
