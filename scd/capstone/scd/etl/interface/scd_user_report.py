from pyspark.sql.functions import col


def create_scd_users_report_view(users_data):
    # Rename columns
    renamed_data = users_data.select(
       col("id"),
       col("is_active"),
       col("start_date"),
       col("end_date"),
       col("name"),
       col("age"),
    )

    # Create or replace a temporary view
    renamed_data.createOrReplaceGlobalTempView("scd_users_report")
