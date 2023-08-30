"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='transit.stations.turnstile',
    KEY='station_id',
    VALUE_FORMAT='avro'
);

CREATE TABLE turnstile_summary
WITH (
    VALUE_FORMAT='json'
) AS
    SELECT
        station_id,
        COUNT(*) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    response = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        response.raise_for_status()
    except requests.exceptions.RequestException:
        logger.error(f"Failed to executed KSQL statement. Error: {json.dumps(response.json())}")


if __name__ == "__main__":
    execute_statement()
