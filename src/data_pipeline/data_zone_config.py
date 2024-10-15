from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

DEFAULT_BASE_DATA_PATH = Path.home() / "data"
DEFAULT_API_PAGE_SIZE = 100
DEFAULT_MAX_DURATION_S = 600  # 10 minutes


@dataclass
class DataZoneConfig:
    base_data_path: str
    base_api_url: str
    api_page_size: int
    max_duration_s: int
    zone_name: str
    database_name: str
    sql_files_folder_name: str

    @classmethod
    def from_config_file(cls, config_file: Path):
        with open(config_file, "r") as f:
            config_obj = json.load(f)
        return cls(
            base_data_path=config_obj.get("base_data_path", DEFAULT_BASE_DATA_PATH),
            base_api_url=config_obj.get("base_api_url"),
            api_page_size=config_obj.get("api_page_size", DEFAULT_API_PAGE_SIZE),
            max_duration_s=config_obj.get("max_duration_s", DEFAULT_MAX_DURATION_S),
            zone_name=cls.zone_name,
            database_name=cls.database_name,
            sql_files_folder_name=config_obj.get("sql_files_folder_name"),
        )

    @property
    def database_path(self) -> str:
        return f"{self.base_data_path}/{self.zone_name}/{self.database_name}.db"

    @property
    def data_path(self) -> str:
        return f"{self.base_data_path}/{self.zone_name}"


class RawZoneConfig(DataZoneConfig):
    database_name = "raw_zone"
    zone_name = "raw_zone"
