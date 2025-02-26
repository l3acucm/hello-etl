from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from scd.utils.base_table import ETLDataSet, TableETL
from scd.etl.silver import SCDUserSilverETL


class SCDUserGoldETL(TableETL):
    def __init__(
            self,
            spark: SparkSession,
            upstream_table_names: Optional[List[Type[TableETL]]] = [SCDUserSilverETL],
            name: str = "user",
            primary_keys: List[str] = ["user_id"],
            storage_path: str = "s3a://hello-data-terraform-backend/delta/gold/user",
            data_format: str = "delta",
            database: str = "scd",
            partition_keys: List[str] = [],
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
        transformed_data = upstream_datasets[0].curr_data
        # do nothing on gold stage
        # Create a new ETLDataSet instance with the transformed data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=transformed_data,
            primary_keys=self.primary_keys,
            storage_path=self.storage_path,
            data_format=self.data_format,
            database=self.database,
            partition_keys=self.partition_keys,
        )

        self.curr_data = etl_dataset.curr_data
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
        user_data = (
            self.spark.read.format(self.data_format)
            .load(self.storage_path)
        )

        user_data = user_data.select(
            col("id"),
            col("name"),
            col("age"),
            col("start_date"),
            col("end_date"),
            col("is_active"),
        )

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
