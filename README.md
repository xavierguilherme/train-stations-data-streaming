#### Chicago Transit Authority Streaming Event Pipeline

This project demonstrates a streaming event pipeline using public data from the [Chicago Transit Authority](https://www.transitchicago.com/data/) to provide insights into train line statuses, movement, turnstile usage, and real-time weather updates. Kafka Connect, KSQL, and Faust are some of the key tools utilized to enable seamless data streaming and real-time processing.

##### Project Architecture

![Image Alt Text](https://video.udacity-data.com/topher/2019/July/5d320154_screen-shot-2019-07-19-at-10.43.38-am/screen-shot-2019-07-19-at-10.43.38-am.png)

##### Prerequisites

Ensure you have the following prerequisites:

- Docker
- Python 3.7

##### Running the Simulation

Follow these steps to set up and run the project locally:

```bash
# Start Docker Containers
docker-compose up -d

# Run the Producer
python producers/simulation.py

# Run the Faust Stream Processing Application
cd consumers && faust -A faust_stream worker -l info

# Run the KSQL Creation Script
python consumers/ksql.py

# Run the Consumer
python consumers/server.py
```

Access the Real-time UI by opening your browser and navigating to http://localhost:8888.

This project is part of the Udacity Data Streaming Nanodegree.