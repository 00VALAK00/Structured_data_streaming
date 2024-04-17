import logging
import json
from confluent_kafka import Producer
import pandas as pd
import time

data_path = "data/sensors.csv"
logging.basicConfig(level=logging.INFO)

def create_dataframe(data: str):
    sensors = pd.read_csv(data_path, sep=",",
                          na_filter=True)
    return sensors


def stream_df_to_kafka():
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    df = create_dataframe(data_path)
    end = time.time() + 300
    numb_records_sent = 0
    while True:

        if time.time() > end:
            break
        records=df.sample(200)

        for _,record in records.iterrows():
            json_record=record.to_json()
            json_record=json.loads(json_record)
            producer.produce(topic="sensors_topic", value=json.dumps(json_record).encode("utf-8"))
            logging.info(f"sending record {json_record}")
            producer.flush()
            numb_records_sent += 1
        logging.info(f"{numb_records_sent} records sent to topic")
        time.sleep(100)




def main():
    stream_df_to_kafka()


if __name__ == "__main__":
    main()

# %%
