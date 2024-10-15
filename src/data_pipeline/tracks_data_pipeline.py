from src.data_pipeline.data_pipeline_base import DataPipelineBase


class TracksDataPipeline(DataPipelineBase):

    def __init__(self, table_name: str) -> None:
        super().__init__(table_name)
    
    def load_raw() -> None:
        pass