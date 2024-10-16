from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List
from unittest import TestCase
from unittest.mock import patch

import polars as pl

from src.data_pipeline.data_pipeline_base import DataPipelineBase
from src.data_pipeline.listen_history_data_pipeline import ListenHistoryDataPipeline
from test.utils import delete_test_data

LISTEN_HISTORY_FAKE_RESPONSE = {
    "items": [
        {
            "user_id": 52903,
            "items": [60358, 67443, 34160, 88053, 37988],
            "created_at": "2024-07-09T18:21:06",
            "updated_at": "2024-09-21T21:39:40",
        },
        {
            "user_id": 22785,
            "items": [60358, 77703, 77933, 25306, 57948],
            "created_at": "2022-11-11T06:49:17",
            "updated_at": "2024-09-02T19:19:08",
        },
        {
            "user_id": 96781,
            "items": [43095, 65624, 85458, 86674, 59490],
            "created_at": "2024-05-29T01:47:47",
            "updated_at": "2024-08-02T11:13:03",
        },
    ],
    "total": 3,
    "page": 1,
    "size": 3,
    "pages": 1,
}


class TestListenHistoryDataPipeline(TestCase):
    _listen_history_columns = list(LISTEN_HISTORY_FAKE_RESPONSE["items"][0].keys())  # type: ignore
    _dummy_table = "listen_history_dummy_table"
    _data_config_path = Path(__file__).parent / "test_data_config.json"

    def setUp(self) -> None:
        self.listen_history_pipeline = ListenHistoryDataPipeline(self._dummy_table, self._data_config_path)

    def tearDown(self) -> None:
        delete_test_data(Path(self.listen_history_pipeline._raw_zone_config.base_data_path))

    def test_loading_into_delta_table(self) -> None:
        with patch.object(DataPipelineBase, "_get_response_data_from_api") as get_api_response_mock:
            get_api_response_mock.return_value = LISTEN_HISTORY_FAKE_RESPONSE
            self.listen_history_pipeline.run()

            delta_table = pl.read_delta(self.listen_history_pipeline.data_path)
            self.assertEqual(list(delta_table.columns), self._listen_history_columns)
            self.assertEqual(len(delta_table), len(LISTEN_HISTORY_FAKE_RESPONSE["items"]))

    def test_adding_history_for_user(self) -> None:
        with patch.object(DataPipelineBase, "_get_response_data_from_api") as get_response_mock:
            get_response_mock.return_value = LISTEN_HISTORY_FAKE_RESPONSE
            self.listen_history_pipeline.run()

            delta_table = pl.read_delta(self.listen_history_pipeline.data_path)
            user_id = 52903
            initial_user_history = self._get_history_for_user(delta_table, user_id)

            new_item_id = 1234

            get_response_mock.return_value = self._add_listen_history_for_user(user_id, new_item_id)
            self.listen_history_pipeline.run()
            delta_table = pl.read_delta(self.listen_history_pipeline.data_path)
            updated_user_history = self._get_history_for_user(delta_table, user_id)
            expected_user_history = initial_user_history
            expected_user_history.append(new_item_id)
            self.assertEqual(set(updated_user_history), set(expected_user_history))

    @staticmethod
    def _add_listen_history_for_user(user_id: int, item_id: int) -> Dict[str, Any]:
        listen_history_fake_response: Dict[str, Any] = deepcopy(LISTEN_HISTORY_FAKE_RESPONSE)
        for i, item in enumerate(listen_history_fake_response["items"]):
            if item["user_id"] == user_id:
                listen_history_fake_response["items"][i]["items"].append(item_id)

        return listen_history_fake_response

    @staticmethod
    def _get_history_for_user(df: pl.DataFrame, user_id: int) -> List[int]:
        return pl.Series(df.filter(pl.col("user_id") == user_id).select("items")).to_list()[0]
