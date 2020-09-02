"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile_entries (
    timestamp VARCHAR,
    station_id INT,
    station_name VARCHAR,
    line INT
) WITH (
    KAFKA_TOPIC='turnstile.entries',
    VALUE_FORMAT='AVRO',
    KEY='timestamp'
);

CREATE TABLE turnstile_summary
WITH (
    VALUE_FORMAT='JSON'
) AS
    SELECT count(station_id) AS count
    FROM turnstile_entries te
    GROUP BY te.station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.info("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers = { "Content-Type": "application/vnd.ksql.v1+json" },
        data = json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
