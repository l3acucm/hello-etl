from typing import Dict, List, Optional, Type

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from scd.utils.base_table import TableETL, ETLDataSet
from scd.utils.db import get_upstream_table


class SCDUserBronzeETL(TableETL):
    def __init__(
            self,
            spark: SparkSession,
            upstream_table_names: Optional[List[Type[TableETL]]] = None,
            name: str = "user",
            primary_keys: List[str] = ["user_id"],
            storage_path: str = "s3a://hello-data-terraform-backend/delta/bronze/user",
            data_format: str = "delta",
            database: str = "helloscd",
            partition_keys: List[str] = ["updated_at"],
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
        # Assuming user data is extracted from a database or other source
        # and loaded into a DataFrame
        table_name = "scd_users"
        user_data = get_upstream_table(table_name, self.spark)
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

        return [etl_dataset]

    def transform_upstream(
            self, upstream_datasets: List[ETLDataSet]
    ) -> ETLDataSet:
        extracted_data = upstream_datasets[0].curr_data

        # Nothing to do for bronze stage
        # Create a new ETLDataSet instance with the extracted data
        etl_dataset = ETLDataSet(
            name=self.name,
            curr_data=extracted_data,
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
            col("updated_at"),
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
