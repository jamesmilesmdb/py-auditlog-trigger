import gzip
import json
from datetime import datetime
import requests
from requests.auth import HTTPDigestAuth
import pymongo

# URL Components
base_url = "https://cloud.mongodb.com/api/atlas/v1.0/"
group_id = "groups/61ae0f46078f8128ffc6d46b/"
target_cluster = "clusters/cloudwatchsync-shard-00-00.pvy7q.mongodb.net/"

# Construct URL
url = base_url + group_id + target_cluster + 'logs/mongodb-audit-log.gz'

write_url = "mongodb+srv://<USERNAME>:<PASSWORD>@cloudwatchsync.pvy7q.mongodb.net/test?retryWrites=true&w=majority"

while True:
    ts= datetime.today().strftime('%s')
    filename = ts + "_mongodb-audit-log.gz"

    print("Starting Audit Log Sync " + str(ts))

    with open(filename, "wb") as f:
        print("Downloading Audit Log GZIP")
        r = requests.get(url, auth=HTTPDigestAuth('bthchpwu', '9fc1d33b-b674-45a2-87f5-8a14e10c2915'), verify=False, stream=True)      
        f.write(r.content)

    with gzip.open(filename, 'rb') as f:
        print("Extracting Audit Log GZIP")
        file_content = f.read()

    decoded = file_content.decode('UTF-8')

    s = decoded.split('\n')

    d = []
    print("Creating Dictionary Audit Log")
    for i in s:
        try:
            d.append(json.loads(i))
        except Exception as e:
            pass

    print("Inserting changes to Audit Log")

    client = pymongo.MongoClient(write_url)

    collection = client["audit_log"]["logs"]

    for j in range(len(d)):
        d[j]["_id"] = d[j]["uuid"]["$binary"]
        try:
            result = collection.insert_one(d[j])
        except Exception as e: 
            pass



