"""One-time setup for a fresh stack: Kafka topics + seed data in HDFS raw. Safe to run again."""
import os
import re
import sys
import time

import requests
from kafka.admin import KafkaAdminClient, NewTopic

NAMENODE = "http://localhost:9870"
WEBHDFS = f"{NAMENODE}/webhdfs/v1"
DATANODE = "http://localhost:9864"
KAFKA = "localhost:9092"
TOPICS = ["orders", "returns", "complaints"]
SEED_FILES = {"orders": "orders.json", "returns": "returns.json",
              "complaints": "complaints.json", "sellers": "sellers.json"}
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def wait_for(name, check, attempts=36):
    for _ in range(attempts):
        try:
            check()
            print(f"  {name} is ready")
            return
        except Exception:
            time.sleep(5)
    sys.exit(f"{name} is not reachable after 3 minutes. Is the stack running? (./start_platform.sh)")


def hdfs_ready():
    """A live datanode, and the namenode out of safe mode (it refuses writes while starting)."""
    jmx = f"{NAMENODE}/jmx"
    state = requests.get(jmx, params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}, timeout=5).json()
    info = requests.get(jmx, params={"qry": "Hadoop:service=NameNode,name=NameNodeInfo"}, timeout=5).json()
    assert state["beans"][0]["NumLiveDataNodes"] >= 1
    assert info["beans"][0]["Safemode"] == ""


def kafka_ready():
    KafkaAdminClient(bootstrap_servers=KAFKA, request_timeout_ms=5000).close()


def hdfs_exists(path):
    return requests.get(f"{WEBHDFS}{path}", params={"op": "GETFILESTATUS"}, timeout=10).status_code == 200


def hdfs_upload(local_path, hdfs_path):
    # 1) the namenode says which datanode will store the file
    response = requests.put(f"{WEBHDFS}{hdfs_path}", allow_redirects=False, timeout=10,
                            params={"op": "CREATE", "overwrite": "false", "user.name": "root", "noredirect": "true"})
    if response.status_code == 307:
        location = response.headers["Location"]
    else:
        response.raise_for_status()
        location = response.json()["Location"]
    # 2) send the bytes to that datanode (reachable from the laptop as localhost:9864)
    location = re.sub(r"^https?://[^/]+", DATANODE, location)
    with open(local_path, "rb") as file:
        requests.put(location, data=file, timeout=60,
                     headers={"Content-Type": "application/octet-stream"}).raise_for_status()


print("1/3 Waiting for HDFS and Kafka")
wait_for("HDFS", hdfs_ready)
wait_for("Kafka", kafka_ready)

print("2/3 Kafka topics")
admin = KafkaAdminClient(bootstrap_servers=KAFKA)
existing = set(admin.list_topics())
missing = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in TOPICS if t not in existing]
if missing:
    admin.create_topics(new_topics=missing)
for topic in TOPICS:
    print(f"  {topic}: {'already existed' if topic in existing else 'created'}")
admin.close()

print("3/3 Seed data → HDFS raw")
for dataset, filename in SEED_FILES.items():
    hdfs_path = f"/data/raw/{dataset}/{filename}"
    if hdfs_exists(hdfs_path):
        print(f"  {hdfs_path}: already there")
    else:
        hdfs_upload(os.path.join(DATA_DIR, filename), hdfs_path)
        print(f"  {hdfs_path}: uploaded")

print("Bootstrap done. Next: start Flask and Streamlit, then run the pipeline from the app.")
