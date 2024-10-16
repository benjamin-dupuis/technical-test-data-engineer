import shutil
from pathlib import Path


def delete_test_data(data_path: Path) -> None:
    if data_path.exists():
        shutil.rmtree(data_path)
