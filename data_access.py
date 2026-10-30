"""Read analyst-ready (curated) data straight from HDFS, without starting Spark."""
import io
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

WEBHDFS = "http://localhost:9870/webhdfs/v1"   # namenode: knows where the data is
DATANODE = "http://localhost:9864"             # datanode: holds the data (port published to the laptop)


def _list_parquet(folder):
    response = requests.get(f"{WEBHDFS}{folder}", params={"op": "LISTSTATUS"}, timeout=10)
    if response.status_code == 404:
        return []
    response.raise_for_status()
    return [f["pathSuffix"] for f in response.json()["FileStatuses"]["FileStatus"]
            if f["type"] == "FILE" and f["pathSuffix"].endswith(".parquet")]


def _read_file(path):
    # 1) ask the namenode where the file is (noredirect = answer in JSON instead of redirecting)
    response = requests.get(f"{WEBHDFS}{path}", params={"op": "OPEN", "noredirect": "true"}, timeout=10)
    response.raise_for_status()
    location = response.json()["Location"]
    # 2) the namenode names the datanode by its container name; from the laptop it is localhost:9864
    location = re.sub(r"^https?://[^/]+", DATANODE, location)
    data = requests.get(location, timeout=30)
    data.raise_for_status()
    return pq.read_table(io.BytesIO(data.content))


def read_parquet_folder(folder):
    """All Parquet part-files of one HDFS folder, as one pandas DataFrame."""
    tables = [_read_file(f"{folder}/{name}") for name in _list_parquet(folder)]
    if not tables:
        return pd.DataFrame()
    return pa.concat_tables(tables).to_pandas()
