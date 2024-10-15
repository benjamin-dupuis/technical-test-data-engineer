from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import json


@dataclass
class DataZoneConfig:
    base_data_path: str
    base_api_url: str
    database_name: str
    sql_files_folder_name: str

    @classmethod
    def from_config_file(cls, config_file: Path):
        with open(config_file, "r") as f:
            config_obj = json.load(f)
        return cls(
            base_data_path=config_obj.get("base_data_path", Path.home() / "data"),
            base_api_url=config_obj.get("base_api_url"),
            database_name=config_obj.get("database_name", "raw_zone"),
            sql_files_folder_name=config_obj.get("sql_files_folder_name")
        )

    @property
    def database_path(self) -> str:
        return f"{self.base_data_path}/{self.database_name}.db"
