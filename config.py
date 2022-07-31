from modules.definitions import *
from google.cloud import storage
from google.oauth2 import service_account

import json

def mycon():
    with open('.\security\parameters.json', 'r') as json_file:
        parameters = json.load(json_file)
    conection = connect_database(parameters['DB_USER'], parameters['DB_PASS'], parameters['DB_NAME'], parameters['DB_HOST'])
    return conection

def myclient():
    with open('.\security\parameters.json', 'r') as json_file:
        parameters = json.load(json_file)
    bucket_name = parameters['bucket_name']
    credentials = service_account.Credentials.from_service_account_file(".\security\parameters-key.json")
    storage_client = storage.Client(credentials=credentials)
    #bucket = storage_client.bucket(bucket_name)
    return storage_client, bucket_name