from requests import Session
from google.cloud.pubsub_v1 import PublisherClient
from concurrent import futures
from os import environ
from time import sleep
from google.cloud.pubsub_v1.publisher.futures import Future


crypto_URL = "https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD,JPY,EUR"
API_KEY = "&api_key={}".format("e9401944c705581b9987971c7519a3832a7be845f649e947aa84289599417974")


class publish_PUB_SUB:
    def __init__(self):
        self.project_id = "capstone-week1"
        self.topic_id = "crypto_bank-capstone1"
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []

    def get_crypto_data(self):
        ses = Session()
        result = ses.get(crypto_URL+API_KEY,stream = True)

        if 200 <= result.status_code < 400:
            return result.text
        else:
            raise Exception(f"Failed to fetch API data - {res.status_code}: {res.text}")

    def publish_message_to_topic(self, message: str):
        publish_future = self.publisher_client.publish(self.topic_path, message.encode("utf-8"))
        futures.wait(self.publish_futures, return_when = futures.ALL_COMPLETED)




if __name__ == "__main__":
    svc = publish_PUB_SUB()
    for i in range(21):
        out = svc.get_crypto_data()
        svc.publish_message_to_topic(out)
        #sleep(30)
