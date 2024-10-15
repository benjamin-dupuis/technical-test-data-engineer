from data_pipeline_base import DataPipelineBase
from data_pipeline_base import DataPipelineBase
from api_endpoints import APIEndpoints


class TracksDataPipeline(DataPipelineBase):
    def __init__(self, table_name: str) -> None:
        self._api_endpoint = APIEndpoints.TRACKS
        super().__init__(table_name, self._api_endpoint)

    @property
    def sql_file_name(self) -> str:
        return "tracks.sql"

    @property
    def merge_predicate(self) -> str:
        return "id"


if __name__ == "__main__":
    table_name = APIEndpoints.TRACKS.value
    pipeline = TracksDataPipeline(table_name=table_name)
    pipeline.run()
