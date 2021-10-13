import logging
from base64 import b64decode
from pandas import DataFrame
from json import loads
from google.cloud.storage import Client


class LoadToStorage:
    def __init__(self,event,context):
        self.event = event
        self.context = context
        self.bucket_name = "capstone1-crypto-storage"

    def get_message_data(self):
        if data in self.event:
            pubsub_message = b64decode(self.event['data'].decode("utf-8"))
            return pubsub_message
        else:
            return ""

    def transform_payload_to_dataframe(self, message):
        try:
            df = DataFrame(loads(message))
            return df
        except Exception as e:
            raise


    def upload_to_bucket(self,df, file_name : str = 'payload'):
        storage_client = Client()
        bucket = storage.client.bucket(self.bucket_name)
        blob = bucket.blob(f"{file_name}.csv")
        blob.upload_from_string(data=df.to_csv(index=False), content_type = "text/csv")

def process(event, context):
    svc = LoadToStorage(event,context)
    message = svc.get_message_data()
    upload_df = svc.transform_payload_to_dataframe(message)
    payload_timestamp = upload_df['price_timestamp'].unique().tolist()[0]

    svc.upload_to_bucket(upload_df,'capstone1-crypto-storage'+str(payload_timestamp))
