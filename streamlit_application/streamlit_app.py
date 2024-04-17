import pandas as pd
import streamlit as st
from confluent_kafka import Consumer
import json
import plotly.express as px
import time
import logging
import matplotlib.pyplot as plt


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


def create_consumer():
    config = {"bootstrap.servers": "localhost:9092", "auto.offset.reset": "earliest", 'group.id': 'grp1', }
    consumer = Consumer(**config)
    consumer.subscribe(["sensors_topic"])
    return consumer




def consume_data(consumer):
    data = []
    start = time.time() + 10
    while True:
        if time.time() > start:
            break
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        data.append(json.loads(msg.value().decode("utf-8")))
        logging.info('Received message: {}'.format(msg.value().decode('utf-8')))
    return data


def convert_data_to_dataframe(data):
    df = pd.DataFrame(data)
    return df


def update_data():
    consumer = create_consumer()
    data = consume_data(consumer)
    df = convert_data_to_dataframe(data)
    return df


def visualize_temperature(df: pd.DataFrame):
    fig=plt.plot(df["temperature"],linewidth=2.0,)
    return fig


def test_it_all():
    last_refreshed = st.empty()
    last_refreshed.text(f"last refreshed at {time.strftime('%D-%m-%Y || %H:%M:%S')}")
    st.title("Behold everyone")
    df = update_data()
    logging.info(df["humidity"].head(3))
    logging.info(f"columns are {df.columns}")
    fig = visualize_temperature(df)
    st.pyplot(fig)


test_it_all()
