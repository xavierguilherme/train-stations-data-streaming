"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
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
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("postgres.cta.stations", value_type=Station)
out_topic = app.topic("transit.stations.info", partitions=1)

table = app.Table(
   "transformed_station",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def transform_station(stations):
    async for station in stations:

        station_line = ""
        if station.red:
            station_line = "red"
        elif station.blue:
            station_line = "blue"
        elif station.green:
            station_line = "green"            

        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=station_line
        )

        table[station.station_id] = transformed_station

if __name__ == "__main__":
    app.main()
