import os
import requests
import json
from datetime import datetime
import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableClient
from azure.core.exceptions import ResourceNotFoundError

ADX_CLUSTER = os.getenv('ADX_CLUSTER')
ADX_DATABASE = os.getenv('ADX_DATABASE')
TABLE_NAME = os.getenv('TABLE_NAME')
SENTINEL_WORKSPACE_ID = os.getenv('SENTINEL_WORKSPACE_ID')
SENTINEL_SHARED_KEY = os.getenv('SENTINEL_SHARED_KEY')
STORAGE_CONNECTION_STRING = os.getenv('AzureWebJobsStorage')
STATE_TABLE_NAME = "adxStateTable"
STATE_PARTITION_KEY = "adxState"
STATE_ROW_KEY = "lastProcessedRow"

def get_adx_token():
    try:
        credential = DefaultAzureCredential()
        token = credential.get_token("https://kusto.kusto.windows.net/.default")
        return token.token
    except Exception as e:
        logging.error(f"Error getting ADX token: {str(e)}")
        raise

def get_last_processed_row():
    table_client = TableClient.from_connection_string(STORAGE_CONNECTION_STRING, STATE_TABLE_NAME)
    try:
        entity = table_client.get_entity(partition_key=STATE_PARTITION_KEY, row_key=STATE_ROW_KEY)
        return entity['LastProcessedRow']
    except ResourceNotFoundError:
        logging.warning(f"No previous state found in {STATE_TABLE_NAME}.")
        return None
    except Exception as e:
        logging.error(f"Error getting last processed row: {str(e)}")
        raise

def update_last_processed_row(row_key):
    table_client = TableClient.from_connection_string(STORAGE_CONNECTION_STRING, STATE_TABLE_NAME)
    entity = {
        'PartitionKey': STATE_PARTITION_KEY,
        'RowKey': STATE_ROW_KEY,
        'LastProcessedRow': row_key
    }
    try:
        table_client.upsert_entity(entity)
    except Exception as e:
        logging.error(f"Error updating last processed row: {str(e)}")
        raise

def query_adx(token, last_processed_row):
    url = f"{ADX_CLUSTER}/v1/rest/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    csl = f"{TABLE_NAME} | where Timestamp > datetime({last_processed_row})" if last_processed_row else f"{TABLE_NAME}"
    body = {
        "db": ADX_DATABASE,
        "csl": csl
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error querying ADX: {str(e)}")
        raise

def send_to_sentinel(data):
    url = f"https://{SENTINEL_WORKSPACE_ID}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01"
    headers = {
        "Authorization": f"SharedKey {SENTINEL_WORKSPACE_ID}:{SENTINEL_SHARED_KEY}",
        "Content-Type": "application/json"
    }
    for record in data:
        body = {
            "time": datetime.utcnow().isoformat() + "Z",
            "data": record
        }
        try:
            response = requests.post(url, headers=headers, data=json.dumps(body))
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Error sending data to Sentinel: {str(e)}")
            raise

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started.')

    try:
        token = get_adx_token()
        last_processed_row = get_last_processed_row()
        adx_data = query_adx(token, last_processed_row)
        
        if adx_data['Tables'][0]['Rows']:
            send_to_sentinel(adx_data['Tables'][0]['Rows'])
            new_last_processed_row = adx_data['Tables'][0]['Rows'][-1][0]  # Assuming the first column is the timestamp
            update_last_processed_row(new_last_processed_row)
        else:
            logging.info('No new data to process.')

    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")