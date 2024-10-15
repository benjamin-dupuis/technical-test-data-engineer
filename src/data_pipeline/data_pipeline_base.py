import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

import duckdb
import polars as pl
import requests  # type: ignore
from deltalake import DeltaTable
from polars import DataFrame
import logging

from src.data_pipeline.api_endpoints import APIEndpoints
from src.data_pipeline.data_zone_config import DEFAULT_DATA_CONFIG, RawZoneConfig

from src.data_pipeline.get_logger import logger

class DataPipelineBase(ABC):
    def __init__(
        self, table_name: str, api_endpoint: APIEndpoints, data_zone_config_file: Path = DEFAULT_DATA_CONFIG
    ) -> None:
        self._api_endpoint = api_endpoint
        self._table_name = table_name
        self._raw_zone_config = RawZoneConfig.from_config_file(data_zone_config_file)

    def run(self) -> None:
        self.load_raw()

    @property
    def data_path(self) -> str:
        return rf"{self._raw_zone_config.data_path}/{self._table_name}"

    @property
    @abstractmethod
    def merge_predicate(self) -> str:
        pass

    def get_endpoint_url(self, page: int) -> str:
        return (
            f"{self._raw_zone_config.base_api_url}/"
            f"{self._api_endpoint.value}"
            f"?page={page}&size={self._raw_zone_config.api_page_size}"
        )

    def write_to_delta_table(self, df: DataFrame) -> None:
        if not DeltaTable.is_deltatable(self.data_path):
            print("Write to delta")
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
        logger.info("Loading Raw data...")
        items_count = 0
        page = 1
        total_items = self._get_response_data_from_api(page).get("total")
        timeout = time.time() + self._raw_zone_config.max_duration_s
        fully_loaded_data: DataFrame = pl.DataFrame()
        if isinstance(total_items, int):
            try:
                while True and items_count < total_items and time.time() < timeout:
                    logger.info(f"Loading page {page} from API for {self._api_endpoint.value}...")
                    df: DataFrame = self._generate_dataframe_from_response(page)
                    fully_loaded_data = pl.concat([fully_loaded_data, df])

                    page += 1
                    items_count += len(df)

                self.write_to_delta_table(fully_loaded_data)

            except Exception as e:
                logger.error(f"An error occurred when loading Raw data: {str(e)}")




    def _generate_dataframe_from_response(self, page: int) -> DataFrame:
        data = self._get_response_data_from_api(page)
        if data:
            return pl.DataFrame(data=data.get("items")).cast({"created_at": pl.Datetime, "updated_at": pl.Datetime})
        else:
            return pl.DataFrame()

    def _get_response_data_from_api(self, page: int) -> Dict[str, Any]:
        endpoint_url = self.get_endpoint_url(page)
        try:
            response = requests.get(endpoint_url)
            return response.json()


        except requests.exceptions.Timeout as timeout_error:
            logger.error(f"Timeout error occurred from server with URL '{endpoint_url}': {timeout_error}")
            return {}

        except requests.exceptions.TooManyRedirects as too_many_requests:
            logger.error(f"Too many request sent to '{endpoint_url}': {too_many_requests}")
            return {}

        except requests.exceptions.RequestException as request_exception:
            logger.error(f"A request exception error occurred from server with URL '{endpoint_url}': {request_exception}")
            return {}

        except Exception as error:
            logger.error(f"An error occurred from server with URL '{endpoint_url}': {error}")
            return {}
