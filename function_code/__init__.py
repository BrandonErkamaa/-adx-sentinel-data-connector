import os
import requests
import json
from datetime import datetime
import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential

ADX_CLUSTER = os.getenv('ADX_CLUSTER')
ADX_DATABASE = os.getenv('ADX_DATABASE')
TABLE_NAME = os.getenv('TABLE_NAME')
SENTINEL_WORKSPACE_ID = os.getenv('SENTINEL_WORKSPACE_ID')
SENTINEL_SHARED_KEY = os.getenv('SENTINEL_SHARED_KEY')

def get_adx_token():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://kusto.kusto.windows.net/.default")
    return token.token

def query_adx(token):
    url = f"{ADX_CLUSTER}/v1/rest/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    body = {
        "db": ADX_DATABASE,
        "csl": f"{TABLE_NAME} | where <conditions>"
    }
    response = requests.post(url, headers=headers, data=json.dumps(body))
    response.raise_for_status()
    return response.json()

def send_to_sentinel(data):
    url = f"https://{SENTINEL_WORKSPACE_ID}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"
    headers = {
        "Authorization": f"SharedKey {SENTINEL_WORKSPACE_ID}:{SENTINEL_SHARED_KEY}",
        "Content-Type": "application/json"
    }
    body = {
        "time": datetime.utcnow().isoformat() + "Z",
        "data": data
    }
    response = requests.post(url, headers=headers, data=json.dumps(body))
    response.raise_for_status()
    return response.status_code

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        token = get_adx_token()
        adx_data = query_adx(token)
        send_to_sentinel(adx_data)
        return func.HttpResponse("Success", status_code=200)
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse("Error", status_code=500)