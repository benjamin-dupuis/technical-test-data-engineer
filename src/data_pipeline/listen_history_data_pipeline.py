from data_pipeline_base import DataPipelineBase
from data_pipeline_base import DataPipelineBase
from api_endpoints import APIEndpoints, DEFAULT_URL
import requests
import polars as pl
from polars import DataFrame, col, concat
from deltalake import DeltaTable
from typing import Dict, Any
import duckdb




class ListenHistoryDataPipeline(DataPipelineBase):

    def __init__(self, table_name: str) -> None:
        self._api_endpoint = APIEndpoints.LISTEN_HISTORY
        super().__init__(table_name, self._api_endpoint)

    @property
    def sql_file_name(self) -> str:
        return "listen_history.sql"
        
    @property
    def merge_predicate(self) -> str:
        return "user_id"     

    def write_to_delta_table(self, df: DataFrame) -> None:
        if not DeltaTable.is_deltatable(self.data_path):
            df.write_delta(self.data_path)
        else:
            (
                df.write_delta(
                self.data_path,
                mode="merge",
                delta_merge_options={
            "predicate": f"s.{self.merge_predicate} = t.{self.merge_predicate}",
            "source_alias": "s",
            "target_alias": "t",
        }
                )
                .when_not_matched_insert_all()
                .when_matched_update(updates={"items": concat(col("s.items"), col("t.items"))})
                .execute()
                )
        

if __name__ == "__main__":
    table_name = APIEndpoints.LISTEN_HISTORY.value
    pipeline = ListenHistoryDataPipeline(table_name=table_name)
    pipeline.run()