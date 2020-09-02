"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema = None,
        num_partitions = 1,
        num_replicas = 1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name        = topic_name
        self.key_schema        = key_schema
        self.value_schema      = value_schema
        self.num_partitions    = num_partitions
        self.num_replicas      = num_replicas
        self.broker_properties = {
            'bootstrap.servers': 'PLAINTEXT://127.0.0.1:9092'
        }

        self.admin_client = AdminClient(self.broker_properties)

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            {
                'bootstrap.servers': self.broker_properties['bootstrap.servers'],
                'schema.registry.url': 'http://localhost:8081'
            },
            self.key_schema,
            self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        topic = NewTopic(
            self.topic_name,
            num_partitions     = self.num_partitions,
            replication_factor = self.num_replicas
        )

        for topic_name, f in self.admin_client.create_topics([topic]).items():
            try:
                f.result()
                print(f"Topic {self.topic_name} created")
            except Exception as e:
                print(f"Failed to create topic {self.topic_name}: {e}")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.info(f"Flushing '{self.topic_name}' producer...")
        remaining = self.producer.flush(10)
        if remaining > 0:
            logger.info(f"\t...timed out, {remaining} messages remaining in queue")
        else:
            logger.info("\t...done")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
