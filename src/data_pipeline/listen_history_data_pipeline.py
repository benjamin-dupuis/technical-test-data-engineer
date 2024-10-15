import polars as pl
from api_endpoints import APIEndpoints
from data_pipeline_base import DataPipelineBase
from deltalake import DeltaTable
from polars import DataFrame


class ListenHistoryDataPipeline(DataPipelineBase):
    def __init__(self, table_name: str) -> None:
        self._api_endpoint = APIEndpoints.LISTEN_HISTORY
        super().__init__(table_name, self._api_endpoint)

    @property
    def sql_file_name(self) -> str:
        return "listen_history.sql"

    @property
    def merge_predicate(self) -> str:
        return "user_id"

    def write_to_delta_table(self, df: DataFrame) -> None:
        if not DeltaTable.is_deltatable(self.data_path):
            df.write_delta(self.data_path)
        else:
            delta_df = pl.read_delta(self.data_path)
            df = df.rename({col: f"s.{col}" for col in df.columns})
            delta_df = delta_df.rename({col: f"t.{col}" for col in delta_df.columns})

            merged_df = delta_df.join(
                df, left_on=f"t.{self.merge_predicate}", right_on=f"s.{self.merge_predicate}", how="left"
            )

            merged_df = (
                merged_df.with_columns(
                    [
                        pl.when(pl.col("s.items").is_not_null())
                        .then(pl.concat_list([pl.col("t.items"), pl.col("s.items")]))
                        .otherwise(pl.col("t.items"))
                        .alias("items")
                    ]
                )
                .select([col for col in delta_df.columns])
                .rename({col: col.replace("t.", "") for col in delta_df.columns})
            )

        merged_df.write_delta(self.data_path, mode="overwrite")


if __name__ == "__main__":
    table_name = APIEndpoints.LISTEN_HISTORY.value
    pipeline = ListenHistoryDataPipeline(table_name=table_name)
    pipeline.run()
