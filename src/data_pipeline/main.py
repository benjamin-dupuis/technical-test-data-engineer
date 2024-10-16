from src.data_pipeline.api_endpoints import APIEndpoints
from src.data_pipeline.listen_history_data_pipeline import ListenHistoryDataPipeline
from src.data_pipeline.tracks_data_pipeline import TracksDataPipeline
from src.data_pipeline.users_data_pipeline import UsersDataPipeline

if __name__ == "__main__":
    tracks_pipeline = TracksDataPipeline(table_name=APIEndpoints.TRACKS.value)
    tracks_pipeline.run()

    users_pipeline = UsersDataPipeline(table_name=APIEndpoints.USERS.value)
    users_pipeline.run()

    listen_history_pipeline = ListenHistoryDataPipeline(table_name=APIEndpoints.LISTEN_HISTORY.value)
    listen_history_pipeline.run()
