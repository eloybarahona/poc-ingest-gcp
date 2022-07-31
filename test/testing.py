from google.cloud import storage
from google.oauth2 import service_account
from more_itertools import bucket
import json
import os


with open('.\security\parameters.json', 'r') as json_file:
    parameters = json.load(json_file)

bucket_name = parameters['bucket_name']

credentials = service_account.Credentials.from_service_account_file(".\security\parameters-key.json")
storage_client = storage.Client(credentials=credentials)

blobs = storage_client.list_blobs(bucket_name)

print(bucket)



