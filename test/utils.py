
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







