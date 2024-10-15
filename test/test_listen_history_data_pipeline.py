from unittest import TestCase
from mock import patch
from src.data_pipeline.listen_history_data_pipeline import ListenHistoryDataPipeline
from src.data_pipeline.data_pipeline_base import DataPipelineBase
from typing import Any, Dict, List
from pathlib import Path
import polars as pl
from unittest.mock import Mock, patch
from test.utils import delete_test_data, DataPipelineBaseTestClass
import json
import shutil

class TestListenHistoryDataPipeline(DataPipelineBaseTestClass):

    _listen_history_fake_response = {
  "items": [
    {
      "user_id": 52903,
      "items": [
        60358,
        67443,
        34160,
        88053,
        37988
      ],
      "created_at": "2024-07-09T18:21:06",
      "updated_at": "2024-09-21T21:39:40"
    },
    {
      "user_id": 22785,
      "items": [
        60358,
        77703,
        77933,
        25306,
        57948
      ],
      "created_at": "2022-11-11T06:49:17",
      "updated_at": "2024-09-02T19:19:08"
    },
    {
      "user_id": 96781,
      "items": [
        43095,
        65624,
        85458,
        86674,
        59490
      ],
      "created_at": "2024-05-29T01:47:47",
      "updated_at": "2024-08-02T11:13:03"
    }
  ],
  "total": 3,
  "page": 1,
  "size": 3,
  "pages": 1
}

    _listen_history_columns = list(_listen_history_fake_response["items"][0].keys())

    def test_loading_into_delta_table(self) -> None:
        with patch.object(DataPipelineBase, "_get_response_data_from_api", self._get_api_response_mock):
            with patch.object(DataPipelineBase, "create_tables", self._create_tables_mock):
                listen_history_pipeline = ListenHistoryDataPipeline(self._dummy_table, self._data_config_path)
                listen_history_pipeline.run()

                delta_table = pl.read_delta(listen_history_pipeline.data_path)
                self.assertEqual(list(delta_table.columns), self._listen_history_columns)
                self.assertEqual(len(delta_table), 3)

    def test_adding_history_for_user(self) -> None:
        with patch.object(DataPipelineBase, "_get_response_data_from_api", self._get_api_response_mock):
            with patch.object(DataPipelineBase, "create_tables", self._create_tables_mock):
                listen_history_pipeline = ListenHistoryDataPipeline(self._dummy_table, self._data_config_path)
                listen_history_pipeline.run()

                delta_table = pl.read_delta(listen_history_pipeline.data_path)
                user_id = 52903
                initial_user_history = self._get_history_for_user(delta_table, user_id)
                print(delta_table)
                print(initial_user_history)

                new_item_id = 1234

                self._add_listen_history_for_user(user_id, new_item_id)
                print(self._listen_history_fake_response)
                listen_history_pipeline.run()
                delta_table = pl.read_delta(listen_history_pipeline.data_path)
                updated_user_history = self._get_history_for_user(delta_table, user_id)
                expected_user_history = initial_user_history
                expected_user_history.append(new_item_id)
                self.assertEqual(set(updated_user_history), set(expected_user_history))

    def _get_api_response_mock(self, page: int = 1) -> Dict[str, Any]:
        return self._listen_history_fake_response

    def _add_listen_history_for_user(self, user_id: int, item_id: int) -> None:
        for i, item in enumerate(self._listen_history_fake_response["items"]):
            if item["user_id"] == user_id:
                self._listen_history_fake_response["items"][i]["items"].append(item_id)

    @staticmethod
    def _get_history_for_user(df: pl.DataFrame, user_id: int) -> List[int]:
        return pl.Series(df.filter(pl.col("user_id") == user_id).select("items")).to_list()[0]




