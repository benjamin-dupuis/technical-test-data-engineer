from src.data_pipeline.api_endpoints import APIEndpoints
from src.data_pipeline.data_pipeline_base import DataPipelineBase
from pathlib import Path
from src.data_pipeline.data_zone_config import DEFAULT_DATA_CONFIG

class UsersDataPipeline(DataPipelineBase):
    def __init__(self, table_name: str, data_config_file: Path = DEFAULT_DATA_CONFIG) -> None:
        self._api_endpoint = APIEndpoints.USERS
        super().__init__(table_name, self._api_endpoint, data_config_file)

    @property
    def sql_file_name(self) -> str:
        return "users.sql"

    @property
    def merge_predicate(self) -> str:
        return "id"


if __name__ == "__main__":
    table_name = APIEndpoints.USERS.value
    pipeline = UsersDataPipeline(table_name=table_name)
    pipeline.run()
