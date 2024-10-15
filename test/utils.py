
from unittest import TestCase
from mock import patch
from src.data_pipeline.tracks_data_pipeline import TracksDataPipeline
from src.data_pipeline.data_pipeline_base import DataPipelineBase
from typing import Any, Dict
from pathlib import Path
import polars as pl
from unittest.mock import Mock, patch
import json
import shutil

def delete_test_data(data_path: Path) -> None:
    if data_path.exists():
        shutil.rmtree(data_path)



class DataPipelineBaseTestClass(TestCase):
    _dummy_table = "dummy_table"
    _data_config_path = Path(__file__).parent / "test_data_config.json"

    def tearDown(self):
        with open(self._data_config_path, "r") as f:
            config_obj = json.load(f)
        base_data_path = Path(__file__).parent / config_obj.get("base_data_path")
        delete_test_data(base_data_path)

    def _create_tables_mock(self) -> None:
        pass




