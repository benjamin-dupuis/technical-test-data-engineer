from unittest import TestCase
from mock import patch
from src.data_pipeline.tracks_data_pipeline import TracksDataPipeline
from src.data_pipeline.data_pipeline_base import DataPipelineBase
from typing import Any, Dict
from pathlib import Path
import polars as pl
from unittest.mock import Mock, patch
from test.utils import delete_test_data, DataPipelineBaseTestClass
import json
import shutil

class TestTracksDataPipeline(DataPipelineBaseTestClass):

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

    _track_columns = list(_tracks_fake_response["items"][0].keys())

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
                self.assertEqual(len(delta_table), initial_count_expected)


                self._add_track()
                tracks_user_pipeline.run()
                delta_table = pl.read_delta(tracks_user_pipeline.data_path)
                self.assertEqual(len(delta_table), initial_count_expected + 1)

    def _get_api_response_mock(self, page: int = 1) -> Dict[str, Any]:
        return self._tracks_fake_response


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




