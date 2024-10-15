from unittest import TestCase
from mock import patch
from src.data_pipeline.users_data_pipeline import UsersDataPipeline
from src.data_pipeline.data_pipeline_base import DataPipelineBase
from typing import Any, Dict
from pathlib import Path
import polars as pl
from unittest.mock import Mock, patch
import json
import shutil
from time import sleep
from test.utils import DataPipelineBaseTestClass

class TestTracksDataPipeline(DataPipelineBaseTestClass):

    _users_fake_response = {
  "items": [
    {
      "id": 52903,
      "first_name": "David",
      "last_name": "Cline",
      "email": "tdavis@example.net",
      "gender": "Gender nonconforming",
      "favorite_genres": "Jazz",
      "created_at": "2023-12-26T02:12:47",
      "updated_at": "2024-02-07T10:22:54"
    },
    {
      "id": 22785,
      "first_name": "Richard",
      "last_name": "Robinson",
      "email": "tinacross@example.net",
      "gender": "Agender",
      "favorite_genres": "Folk",
      "created_at": "2023-12-09T06:47:50",
      "updated_at": "2024-06-01T23:34:06"
    },
    {
      "id": 96781,
      "first_name": "Andrea",
      "last_name": "Stark",
      "email": "tgreene@example.org",
      "gender": "Genderqueer",
      "favorite_genres": "Metal",
      "created_at": "2024-02-20T10:56:06",
      "updated_at": "2024-02-11T03:48:01"
    }
  ],
  "total": 3,
  "page": 1,
  "size": 3,
  "pages": 1
}

    _users_columns = list(_users_fake_response["items"][0].keys())


    def test_loading_into_delta_table(self) -> None:
        with patch.object(DataPipelineBase, "_get_response_data_from_api", self._get_api_response_mock):
            with patch.object(DataPipelineBase, "create_tables", self._create_tables_mock):
                tracks_user_pipeline = UsersDataPipeline(self._dummy_table, self._data_config_path)
                tracks_user_pipeline.run()

                delta_table = pl.read_delta(tracks_user_pipeline.data_path)
                self.assertEqual(list(delta_table.columns), self._users_columns)
                self.assertEqual(len(delta_table), 3)

    def test_incremental_load(self) -> None:
        with patch.object(DataPipelineBase, "_get_response_data_from_api", self._get_api_response_mock):
            with patch.object(DataPipelineBase, "create_tables", self._create_tables_mock):
                tracks_user_pipeline = UsersDataPipeline(self._dummy_table, self._data_config_path)
                tracks_user_pipeline.run()

                delta_table = pl.read_delta(tracks_user_pipeline.data_path)
                initial_count_expected = 3
                self.assertEqual(len(delta_table), initial_count_expected)


                self._add_user()
                tracks_user_pipeline.run()
                delta_table = pl.read_delta(tracks_user_pipeline.data_path)
                self.assertEqual(len(delta_table), initial_count_expected + 1)


    def _get_api_response_mock(self, page: int = 1) -> Dict[str, Any]:
        return self._users_fake_response

    def _add_user(self):
        new_user = {
      "id": 50488,
      "first_name": "Christopher",
      "last_name": "Rivas",
      "email": "davisronald@example.com",
      "gender": "Genderfluid",
      "favorite_genres": "Hip Hop",
      "created_at": "2023-03-23T06:59:00",
      "updated_at": "2023-12-10T07:55:50"
    }

        self._users_fake_response["items"].append(new_user)
        self._users_fake_response["total"] += 1
        self._users_fake_response["size"] += 1




