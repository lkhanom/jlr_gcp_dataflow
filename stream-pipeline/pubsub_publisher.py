import json
import time
from concurrent import futures
from datetime import datetime
from google.cloud import pubsub_v1
from random import randint
import time


# TODO (change to yours)
PROJECT_ID = "jlr-dl-cat-training"
TOPIC_ID = "temp-2022-jlr-de-dataflow-lippe"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
publish_futures = []


def get_callback(publish_future, data):
    def callback(publish_future):
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback


def create_random_message():
    trip_id = randint(10000, 99999)
    start_date = str(datetime.utcnow())
    start_station_id = randint(200, 205)
    bike_number = randint(100, 999)
    duration_sec = randint(1000, 9999)

    message_json = {
        "trip_id": trip_id,
        "start_date": start_date,
        "start_station_id": start_station_id,
        "bike_number": bike_number,
        "duration_sec": duration_sec,
    }
    return message_json


if __name__ == "__main__":
    while True:
        message_json = create_random_message()
        data = json.dumps(message_json)
        publish_future = publisher.publish(topic_path, data.encode("utf-8"))
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)
        time.sleep(0.5)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with error handler to {topic_path}.")
