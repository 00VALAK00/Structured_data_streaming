from confluent_kafka.admin import NewTopic, AdminClient
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
config = {"bootstrap.servers": "localhost:9092",
          "client.id": "kafka_admin_client"}

logger.info(f"Connecting to kafka client at {str(config.get('bootstrap.servers'))}")
try:
    kafka_admin = AdminClient(config)
    if kafka_admin is not None:
        logger.info("Connection successfully established")
except Exception as e:
    logger.info(f"Something went wrong while connecting to kafka client /n {e}")


def create_topic(topic_name: str):
    if topic_name not in kafka_admin.list_topics().topics:
        new_topic = NewTopic(topic_name)
        kafka_admin.create_topics([new_topic])
        logger.info(f"topic {topic_name} created successfully")

    else:
        logger.info(f"topic {topic_name} already exists")
    return 1


def main():
    create_topic("sensors_topic")


if __name__ == "__main__":
    main()
