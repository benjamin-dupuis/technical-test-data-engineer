import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

import duckdb
import polars as pl
import requests
from api_endpoints import APIEndpoints
from data_zone_config import RawZoneConfig
from deltalake import DeltaTable
from polars import DataFrame

DEFAULT_DATA_CONFIG = Path(__file__).parent / "config_files/local_config.json"


class DataPipelineBase(ABC):
    def __init__(
        self, table_name: str, api_endpoint: APIEndpoints, data_zone_config_file: Path = DEFAULT_DATA_CONFIG
    ) -> None:
        self._api_endpoint = api_endpoint
        self._table_name = table_name
        self._raw_zone_config = RawZoneConfig.from_config_file(data_zone_config_file)

    def run(self) -> None:
        self.load_raw()
        self.create_tables()

    @property
    def data_path(self) -> str:
        return rf"{self._raw_zone_config.data_path}/{self._table_name}"

    @property
    @abstractmethod
    def sql_file_name(self) -> str:
        pass

    @property
    @abstractmethod
    def merge_predicate(self) -> str:
        pass

    def get_endpoint_url(self, page: int) -> str:
        return f"{self._raw_zone_config.base_api_url}/{self._api_endpoint.value}?page={page}&size={self._raw_zone_config.api_page_size}"

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
                    },
                )
                .when_not_matched_insert_all()
                .when_matched_update_all()
                .execute()
            )

    def load_raw(self) -> None:
        items_count = 0
        page = 1
        total_items = self._get_response_data_from_api(page).get("total")
        timeout = time.time() + self._raw_zone_config.max_duration_s
        fully_loaded_data: DataFrame = pl.DataFrame()
        if isinstance(total_items, int):
            try:
                while True and items_count < total_items and time.time() < timeout:
                    df: DataFrame = self._generate_dataframe_from_response(page)
                    fully_loaded_data = pl.concat([fully_loaded_data, df])

                    page += 1
                    items_count += len(df)

                self.write_to_delta_table(fully_loaded_data)

            except Exception as e:
                print(e)

    def create_tables(self) -> None:
        with duckdb.connect(self._raw_zone_config.database_path) as con:
            sql_file_path = (
                Path(__file__).parent / f"{self._raw_zone_config.sql_files_folder_name}/{self.sql_file_name}"
            )
            create_raw_table_statement = self.sql_query_from_file(sql_file_path)
            con.sql(create_raw_table_statement)
            con.table(self._table_name).show()

    def sql_query_from_file(self, sql_file_path: Path) -> str:
        with open(sql_file_path, "r") as f:
            base_sql_query = f.read()

        sql_query = base_sql_query.replace("{table_name}", self._table_name).replace("{data_path}", self.data_path)
        return sql_query

    def _generate_dataframe_from_response(self, page: int) -> DataFrame:
        try:
            data = self._get_response_data_from_api(page)
            return pl.DataFrame(data=data.get("items"))
        except Exception as e:
            print(e)

    def _get_response_data_from_api(self, page: int) -> Dict[str, Any]:
        try:
            endpoint_url = self.get_endpoint_url(page)
            response = requests.get(endpoint_url)
            return response.json()

        except Exception as e:
            print(e)
