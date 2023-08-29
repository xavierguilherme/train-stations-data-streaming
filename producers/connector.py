"""Configures a Kafka Connector for Postgres Station data"""
import os
import json
import logging

import requests
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    response = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if response.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    response = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://host.docker.internal:5432/cta",
               "connection.user": os.getenv("POSTGRES_USER"),
               "connection.password": os.getenv("POSTGRES_PWD"),
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "postgres.cta.",
               "poll.interval.ms": "300000", # Pool every 5 minutes
           }
       }),
    )

    if response.status_code != 201:
        logging.error(f"Something went wrong during connector creation: {json.dumps(response.json())}")

    logging.debug("connector created successfully")

if __name__ == "__main__":
    configure_connector()
