from abc import ABC, abstractmethod
from api_endpoints import APIEndpoints, DEFAULT_URL
import requests
import polars as pl
from polars import DataFrame
from deltalake import DeltaTable
from typing import Dict, Any
from pathlib import Path
import duckdb
from data_zone_config import DataZoneConfig

DEFAULT_DATA_CONFIG = Path(__file__).parent / "local_config.json"


class DataPipelineBase(ABC):


    def __init__(self, table_name: str, api_endpoint: APIEndpoints, data_zone_config_file: Path = DEFAULT_DATA_CONFIG) -> None:
        self._api_endpoint = api_endpoint
        self._table_name = table_name
        self._raw_zone_config = DataZoneConfig.from_config_file(data_zone_config_file)


    def run(self) -> None:
        self.load_raw()
        self.create_tables()

    @property
    def endpoint_url(self) -> str:
        return f"{self._raw_zone_config.base_api_url}/{self._api_endpoint.value}"

    @property
    def data_path(self) -> str:
        return rf"{self._raw_zone_config.base_data_path}/{self._table_name}"

    @property
    @abstractmethod
    def sql_file_name(self) -> str:
        pass

    @property
    @abstractmethod
    def merge_predicate(self) -> str:
        pass

    def load_raw(self) -> None:
        try:
            response = requests.get(self.endpoint_url)
            data: Dict[str, Any] = response.json()
            df: DataFrame = pl.DataFrame(data=data["items"])
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
                    .when_matched_update_all()
                    .execute()
                    )
        
        except Exception as e:
            print(e)


    @abstractmethod
    def create_tables() -> None:
        pass

    def sql_query_from_file(self) -> str:
        sql_file_path = Path(__file__).parent / f"sql_files/{self.sql_file_name}"
        with open(sql_file_path, "r") as f:
            base_sql_query = f.read()

        sql_query = base_sql_query.replace("{table_name}", self._table_name).replace("{data_path}", self.data_path)
        return sql_query
