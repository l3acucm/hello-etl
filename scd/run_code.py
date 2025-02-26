from pyspark.sql import SparkSession

from scd.etl.gold import SCDUserGoldETL
from scd.etl.interface.scd_user_report import create_scd_users_report_view
import great_expectations as gx


def check_is_active_end_date_alignment(df):
    active_violations = df[(df["is_active"] == True) & (df["end_date"].notnull())]
    inactive_violations = df[(df["is_active"] == False) & (df["end_date"].isnull())]
    return len(active_violations) == 0 and len(inactive_violations) == 0


def run_code(spark):
    scd_users_metrics = SCDUserGoldETL(spark=spark)
    # scd_users_metrics = SCDUserGoldETL(spark=spark)
    scd_users_metrics.run()
    create_scd_users_report_view(scd_users_metrics.read().curr_data)
    print("=================================")
    print("SCD2 Users Report")
    print("=================================")

    report = spark.sql("select * from global_temp.scd_users_report")

    print("=================================")
    print("Great Expectations")
    print("=================================")
    context = gx.get_context()

    batch_parameters = {"dataframe": report}

    data_source_name = "my_data_source"
    data_asset_name = "my_data_asset"
    batch_definition_name = "my_batch_definition"
    context.data_sources.add_spark(name=data_source_name)
    data_source = context.data_sources.get(data_source_name)
    data_source.add_dataframe_asset(name=data_asset_name)
    data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)
    data_asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )

    batch_definition =  data_asset.get_batch_definition(batch_definition_name)


    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    (batch.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="id")))
    (batch.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="start_date")))

    (batch.validate(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
        column="start_date",
        strftime_format="%Y-%m-%d %H:%M:%S"
    )))
    (batch.validate(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
        column="end_date",
        strftime_format="%Y-%m-%d %H:%M:%S",
        catch_exceptions=False  # Allows nulls for active r
    )))
    (batch.validate(gx.expectations.ExpectColumnValuesToBeBetween(
        column="age",
        max_value=10
    )))
    # Stop SparkSession
    spark.stop()


if __name__ == "__main__":
    # Create a spark session
    spark = (
        SparkSession.builder.appName("SCD2 from scratch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", "")
        .config("spark.hadoop.fs.s3a.secret.key", "")
        # .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "eu-central-1")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark)
