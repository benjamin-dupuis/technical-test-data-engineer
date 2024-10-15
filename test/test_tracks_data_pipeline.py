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
from time import sleep

class TestTracksDataPipeline(TestCase):

    _tracks_fake_response = {
  "items": [
    {
      "id": 2108,
      "name": "real",
      "artist": "Connie Dixon",
      "songwriters": "Susan Johnson",
      "duration": "43:45",
      "genres": "area",
      "album": "thing",
      "created_at": "2022-12-08T11:36:30",
      "updated_at": "2024-06-17T22:06:29"
    },
    {
      "id": 42326,
      "name": "drug",
      "artist": "Anthony Romero",
      "songwriters": "Michelle Parker",
      "duration": "10:00",
      "genres": "director",
      "album": "enough",
      "created_at": "2023-05-10T00:32:02",
      "updated_at": "2024-09-30T00:19:49"
    },
    {
      "id": 69114,
      "name": "begin",
      "artist": "Elizabeth Davis",
      "songwriters": "Jeremy Krueger",
      "duration": "38:13",
      "genres": "capital",
      "album": "voice",
      "created_at": "2023-01-19T19:16:54",
      "updated_at": "2024-06-16T23:33:19"
    }
  ],
  "total": 3,
  "page": 1,
  "size": 3,
  "pages": 1
    }

    _dummy_table = "dummy_table"
    _data_config_path = Path(__file__).parent / "test_data_config.json"
    _track_columns = ["id", "name", "artist", "songwriters", "duration", "genres", "album", "created_at", "updated_at"]

    def setUp(self) -> None:
        self._mock_data_pipeline_base = Mock()
        self._mock_data_pipeline_return_value = self._mock_data_pipeline_base

    def tearDown(self):
        with open(self._data_config_path, "r") as f:
            config_obj = json.load(f)
        base_data_path = Path(__file__).parent / config_obj.get("base_data_path")
        print(base_data_path)
        self._delete_delta_table(base_data_path)

    def test_loading_into_delta_table(self) -> None:
        with patch.object(DataPipelineBase, "_get_response_data_from_api", self._get_api_response_mock):
            with patch.object(DataPipelineBase, "create_tables", self._create_tables_mock):
                tracks_user_pipeline = TracksDataPipeline(self._dummy_table, self._data_config_path)
                tracks_user_pipeline.run()

                delta_table = pl.read_delta(tracks_user_pipeline.data_path)
                self.assertEqual(list(delta_table.columns), self._track_columns)
                self.assertEqual(len(delta_table), 3)

    def test_incremental_load(self) -> None:
        with patch.object(DataPipelineBase, "_get_response_data_from_api", self._get_api_response_mock):
            with patch.object(DataPipelineBase, "create_tables", self._create_tables_mock):
                tracks_user_pipeline = TracksDataPipeline(self._dummy_table, self._data_config_path)
                tracks_user_pipeline.run()

                delta_table = pl.read_delta(tracks_user_pipeline.data_path)
                initial_count_expected = 3
                self.assertEqual(list(delta_table.columns), self._track_columns)
                self.assertEqual(len(delta_table), initial_count_expected)


                self._add_track()
                tracks_user_pipeline.run()
                delta_table = pl.read_delta(tracks_user_pipeline.data_path)
                self.assertEqual(len(delta_table), initial_count_expected + 1)

                self._delete_delta_table(Path(tracks_user_pipeline.data_path))

    def _get_api_response_mock(self, page: int = 1) -> Dict[str, Any]:
        return self._tracks_fake_response

    def _create_tables_mock(self) -> None:
        pass

    def _add_track(self):
        new_track = {
      "id": 47199,
      "name": "degree",
      "artist": "Anna Moore",
      "songwriters": "Stephanie Barber",
      "duration": "32:45",
      "genres": "space",
      "album": "whole",
      "created_at": "2023-04-07T15:16:13",
      "updated_at": "2024-03-15T23:24:48"
    }

        self._tracks_fake_response["items"].append(new_track)
        self._tracks_fake_response["total"] += 1
        self._tracks_fake_response["size"] += 1

    @staticmethod
    def _delete_delta_table(data_path: Path) -> None:
        if data_path.exists():
            shutil.rmtree(data_path)




