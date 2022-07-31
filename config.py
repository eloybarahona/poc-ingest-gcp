from modules.definitions import *
from google.cloud import storage
from google.oauth2 import service_account
from sqlalchemy import create_engine

import json

with open(mypath('/security/')+'parameters.json', 'r') as json_file:
    parameters = json.load(json_file)

def mycon():
    conection = connect_database(parameters['DB_USER'], parameters['DB_PASS'], parameters['DB_NAME'], parameters['DB_HOST'])
    return conection

def myengine():
    url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(parameters['DB_USER'], parameters['DB_PASS'], parameters['DB_HOST'], parameters['DB_PORT'], parameters['DB_NAME'])
    sqlEngine = create_engine(url)
    connection    = sqlEngine.connect()
    return connection

def myclient():
    bucket_name = parameters['bucket_name']
    credentials = service_account.Credentials.from_service_account_file(mypath('/security/')+'parameters-key.json')
    storage_client = storage.Client(credentials=credentials)
    #bucket = storage_client.bucket(bucket_name)
    return storage_client, bucket_name