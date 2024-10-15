from enum import Enum

DEFAULT_URL = "http://localhost:8000"

class APIEndpoints(Enum):

    LISTEN_HISTORY = "listen_history"
    TRACKS = "tracks"
    USERS = "users"

