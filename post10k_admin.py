import requests
import json
from math import ceil


url = "http://127.0.0.1:8000/admin/bulk"
BATCH_SIZE = 1000  # number of records per request

# Load 10k records
with open("admins_10k.json") as f:
    data = json.load(f)

total_records = len(data)
total_batches = ceil(total_records / BATCH_SIZE)

inserted_records = 0

for i in range(total_batches):
    batch = data[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
    response = requests.post(url, json=batch)

    if response.status_code == 200:
        inserted_records += len(response.json())
        print(f" Batch {i+1}/{total_batches} inserted successfully. Total inserted so far: {inserted_records}")
    else:
        print(f" Batch {i+1}/{total_batches} failed with status code {response.status_code}. Response: {response.text}")
        break

print(f"\nFinished! Total records inserted: {inserted_records}/{total_records}")




