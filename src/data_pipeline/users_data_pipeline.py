from data_pipeline_base import DataPipelineBase
from api_endpoints import APIEndpoints, DEFAULT_URL
import requests
import polars as pl
from polars import DataFrame
from deltalake import DeltaTable
from typing import Dict, Any
import duckdb


class UsersDataPipeline(DataPipelineBase):

    def __init__(self, table_name: str) -> None:
        self._api_endpoint = APIEndpoints.USERS
        super().__init__(table_name, self._api_endpoint)

    @property
    def sql_file_name(self) -> str:
        return "users.sql"
        
    @property
    def merge_predicate(self) -> str:
        return "id"
    
    def create_tables(self) -> None:
        with duckdb.connect(f"{self._raw_zone_config.base_data_path}.db") as con:
            create_raw_table_statement = self.sql_query_from_file()
            con.sql(create_raw_table_statement)
            con.table(self._table_name).show()
        



if __name__ == "__main__":
    table_name = APIEndpoints.USERS.value
    pipeline = UsersDataPipeline(table_name=table_name)
    pipeline.run()
