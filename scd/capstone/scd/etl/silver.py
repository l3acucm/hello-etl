from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType, IntegerType, BooleanType, StructField, StructType, TimestampType

from scd.etl.bronze import SCDUserBronzeETL
from scd.utils.base_table import TableETL, ETLDataSet


class SCDUserSilverETL(TableETL):
    def __init__(
            self,
            spark: SparkSession,
            upstream_table_names: Optional[List[Type[TableETL]]] = [SCDUserBronzeETL],
            name: str = "user",
            primary_keys: List[str] = ["id"],
            storage_path: str = "s3a://hello-data-terraform-backend/delta/silver/user",
            data_format: str = "delta",
            database: str = "helloscd",
            partition_keys: List[str] = ["id"],
            run_upstream: bool = True,
            load_data: bool = True,
    ) -> None:
        super().__init__(
            spark,
            upstream_table_names,
            name,
            primary_keys,
            storage_path,
            data_format,
            database,
            partition_keys,
            run_upstream,
            load_data,
        )

    def extract_upstream(self) -> List[ETLDataSet]:
        upstream_etl_datasets = []
        for TableETLClass in self.upstream_table_names:
            t1 = TableETLClass(
                spark=self.spark,
                run_upstream=self.run_upstream,
                load_data=self.load_data,
            )
            if self.run_upstream:
                t1.run()
            upstream_etl_datasets.append(t1.read())

        return upstream_etl_datasets

    def transform_upstream(
            self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        oltp_df = upstream_datasets[0].curr_data
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("is_active", BooleanType(), True)
        ])

        schema_df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        schema_df.write.option("mergeSchema", "true").format(
            self.data_format
        ).mode("overwrite").partitionBy(['id']).save(
            self.storage_path
        )
        scd2_df = (
            self.spark.read.format(self.data_format)
            .load("s3a://hello-data-terraform-backend/delta/gold/user")
        )
        # scd2_df = self.read().curr_data
        print("Delta df before transformation:")
        scd2_df.show()
        # new scd2 records
        new_records = ((((
                             oltp_df.withColumnRenamed("name", "new_name").withColumnRenamed("age", "new_age").alias(
                                 "oltp")
                             .join(
                                 scd2_df.filter(col('is_active') == True).alias("scd2"),
                                 on="id",
                                 how="left"
                             ).filter(
                                 ((col("oltp.new_name") != col("scd2.name")) | (col("scd2.name").isNull())) |
                                 ((col("oltp.new_age") != col("scd2.age")) | (col("scd2.age").isNull()))
                             )
                         ).withColumn('start_date', col("oltp.updated_at"))
                         .withColumn('is_active', lit(True))
                         )
                        .drop('updated_at', 'name', 'age'))
                       .withColumnRenamed("new_name", "name")
                       .withColumnRenamed("new_age", "age"))
        print("new records:")
        new_records.show()

        # rename fields to join df with current scd2 df for further fields adjusting
        new_records_to_join = (
            new_records
            .withColumnRenamed('start_date', 'new_start_date')
            .select('id', 'new_start_date')
        )

        # to be stored in delta (UNION)
        updated_stored_sc2_df = (
            scd2_df.join(
                new_records_to_join, 'id', 'left'
            ).withColumn(
                'end_date',
                when(col('new_start_date').isNotNull(), col('new_start_date')).otherwise(col('end_date'))
            ).withColumn(
                'is_active',
                when(col('new_start_date').isNotNull(), lit(False)).otherwise(col('is_active'))
            )
        ).drop('new_start_date')

        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=updated_stored_sc2_df.union(new_records),
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset

    def read(
            self, partition_values: Optional[Dict[str, str]] = None
    ) -> ETLDataSet:
        if not self.load_data:
            return ETLDataSet(
                name=self.name,
                curr_data=self.curr_data,
                primary_keys=self.primary_keys,
                storage_path=self.storage_path,
                data_format=self.data_format,
                database=self.database,
                partition_keys=self.partition_keys,
            )

        # Read the user data from the Delta Lake table
        user_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
        )
        # Explicitly select columns
        user_data = user_data.select(
            col("id"),
            col("name"),
            col("age"),
            col("start_date"),
            col("end_date"),
            col("is_active"),
        )

        # Create an ETLDataSet instance
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=user_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        return etl_dataset
