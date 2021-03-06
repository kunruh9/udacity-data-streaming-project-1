"""Defines trends calculations for stations"""
import logging
import faust
import topic_names as TOPIC

from dataclasses import dataclass

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str = 'blue'


app       = faust.App("stations-stream", broker = "kafka://localhost:9092", store = "memory://")
topic     = app.topic(TOPIC.STATIONS, value_type = Station)
out_topic = app.topic(TOPIC.TRANSFORMED_STATIONS, value_type = TransformedStation, key_type = int, partitions = 1)
table = app.Table(
   TOPIC.TRANSFORMED_STATIONS,
   default = str,
   partitions = 1,
   changelog_topic = out_topic
)

@app.agent(topic)
async def transform_stations(stations):
    async for station in stations:
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order
        )

        if station.red:
            transformed_station.line = 'red'
        elif station.green:
            transformed_station.line = 'green'

        table[station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
