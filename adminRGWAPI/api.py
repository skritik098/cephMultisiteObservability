import requests
from requests_aws4auth import AWS4Auth

# RGW Admin credentials
access_key = "OP4LVE6KYAF6HS45V1B6"
secret_key = "FYZbSZvb7T1Wr90QAihJREVNUEEOLgGfBDEFuxku"

# IMPORTANT:
# Region can be anything but must match what RGW expects.
# Default often works as "us-east-1"
region = "us-east-1"
service = "s3"

auth = AWS4Auth(access_key, secret_key, region, service)

rgw_host = "http://ceph7-node3:8082"
bucket_name = "test-bucket-1"

url = f"{rgw_host}/admin/bucket"

params = {
    #"bucket": bucket_name,
    "stats": "true",
    "format": "json"
}

response = requests.get(url, auth=auth, params=params)

print("Status:", response.status_code)
print("Raw Output:", response.text)

if response.status_code == 200:
    data = response.json()
    for d in data:
        print("Bucket Name:", d["bucket"])
        print("Owner:", d["owner"])
        print("Size (bytes):", d["usage"]["rgw.main"]["size"])
        print("Num Objects:", d["usage"]["rgw.main"]["num_objects"])
