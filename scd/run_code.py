from pyspark.sql import SparkSession

from rainforest.etl.gold.daily_category_metrics import \
    DailyCategoryMetricsGoldETL
from rainforest.etl.interface.daily_category_report import \
    create_daily_category_report_view


def run_code(spark):
    print("=================================")
    print("Daily Category Report")
    print("=================================")
    daily_cat_metrics = DailyCategoryMetricsGoldETL(spark=spark)
    daily_cat_metrics.run()
    create_daily_category_report_view(daily_cat_metrics.read().curr_data)
    spark.sql("select * from global_temp.daily_category_report").show()


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
