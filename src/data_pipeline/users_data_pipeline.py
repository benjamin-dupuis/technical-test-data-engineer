from data_pipeline_base import DataPipelineBase
from api_endpoints import APIEndpoints


class UsersDataPipeline(DataPipelineBase):
    def __init__(self, table_name: str) -> None:
        self._api_endpoint = APIEndpoints.USERS
        super().__init__(table_name, self._api_endpoint)

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
