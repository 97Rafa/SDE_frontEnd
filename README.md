# SDE Front End

A FastAPI service that exposes the Synopses Data Engine (SDE) as a usable HTTP API. The project sits between external clients and Kafka-backed processing, validating incoming payloads with Pydantic, publishing requests to Kafka topics, buffering state in a local SQLite database, and returning results back to clients.

This backend is designed to make an older Kafka-oriented system easier to consume through familiar REST endpoints instead of requiring direct Kafka integration.

## Overview

The service is responsible for:

- accepting requests from clients through REST endpoints
- validating request and data payloads with Pydantic models
- producing messages to Kafka topics for SDE processing
- consuming Kafka response/log messages
- persisting synopsis and estimation metadata in SQLite
- returning processed results or cached estimations to API callers

## Architecture

The application is organized around a few simple layers:

- FastAPI HTTP layer: routes for data ingestion, synopsis management, snapshots, and estimation requests
- Kafka producer/consumer layer: communication with request, data, estimation, and log topics
- SQLite database layer: lightweight persistence for synopsis entries and cached estimations
- Pydantic schemas: strict validation for request bodies, payload shape, and required fields

## Stack

- Python 3.11
- FastAPI
- Uvicorn
- SQLAlchemy
- SQLite
- Kafka via aiokafka
- Pydantic + Pydantic Settings
- Docker + Docker Compose

## Project structure

```text
.
├── app/
│   ├── config.py
│   ├── database.py
│   ├── kafka_consumer.py
│   ├── kafka_producer.py
│   ├── main.py
│   ├── models.py
│   └── schemas.py
├── uploads/
│   └── MOCK_DATA.csv
├── .env.example
├── create_topics.sh
├── docker-compose.yml
├── Dockerfile
├── locustFile.py
├── requirements.txt
├── README.md
├── SDE.db
└── ...
```

## Configuration

Application defaults are defined in `app/config.py` and can be overridden with environment variables or a `.env` file.

Default Kafka settings:

- `KAFKA_BROKER`: `kafka1:9092`
- `req_topic`: `request_topic`
- `dat_topic`: `data_topic`
- `est_topic`: `estimation_topic`
- `log_topic`: `logging_topic`

Default timeouts:

- `response_timeout`: `6` seconds
- `estimation_timeout`: `5` seconds
- `parallelism`: `4`

The app uses SQLite via `sqlite:///./SDE.db`.

## Running the project

### Using Docker Compose

```bash
docker compose up --build
```

This starts the app together with the Kafka/Zookeeper ecosystem and additional tooling.

### Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 4000
```

The API will then be available at:

- http://localhost:4000
- Swagger docs: http://localhost:4000/docs
- OpenAPI schema: http://localhost:4000/openapi.json

## Kafka topics

The service interacts with these topics:

- `request_topic` — requests sent to SDE
- `data_topic` — raw data ingestion events
- `estimation_topic` — estimation results
- `logging_topic` — response/log messages used to correlate requests

The Kafka topics are initialized by `create_topics.sh` during compose startup.

## Main API endpoints

### Data ingestion

- `POST /dataIn/` — send a validated `DataIn` payload directly to Kafka
- `GET /dataIn/` — read data currently buffered in Kafka
- `POST /dataIn/csv` — upload a CSV file and forward each row as Kafka data payloads

### Synopsis management

- `POST /requests/add` — create a new synopsis request
- `POST /requests/delete` — delete a synopsis
- `GET /synopsis/` — list synopsis rows currently stored in the DB

### Snapshots

- `POST /requests/createSnapshot`
- `POST /requests/listSnapshots`
- `POST /requests/loadLatest`
- `POST /requests/loadCustom`
- `POST /requests/createFromSnap`

### Estimations

- `POST /estimations/` — request or refresh an estimation
- `GET /estimations/` — list estimation cache entries

### Kafka utilities

- `POST /produce/{topic}` — send a raw dict message to a Kafka topic
- `GET /consume/{topic}` — read recent buffered messages from a topic

## Data models

### `RequestBase`

Base request model used across many endpoints.

Fields include:

- `externalUID`
- `uid`
- `streamID`
- `synopsisID`
- `dataSetkey`
- `noOfP`
- `requestID`

### `AddRequest`

Extends the base request with a `param` list used to provide synopsis configuration parameters.

### `EstRequest`

Includes:

- `uid`
- `param`
- `cache_max_age`

### `DataIn`

Contains:

- `streamID`
- `dataSetkey`
- `values`

## Database model summary

The SQLite database stores two main tables:

- `synopsis`
  - `uid`
  - `createdAt`
  - `details`

- `estimations`
  - `uid`
  - `body`
  - `fetchedEst`
  - `last_req`
  - `last_data`

This database acts as a lightweight operational buffer and cache layer, while Kafka remains the primary messaging backbone.

## Example request

```bash
curl -X POST "http://localhost:4000/requests/add" \
  -H "Content-Type: application/json" \
  -d '{
    "uid": 1234,
    "streamID": "streamA",
    "synopsisID": 1,
    "dataSetkey": "Forex",
    "noOfP": 4,
    "param": ["countMin", "ValueField", "Queryable", 10, 0, 1]
  }'
```

## Notes

- This service is designed as a front-end facade for an older Kafka-heavy system.
- Kafka is used as the transport and coordination layer; the DB stores operational state and cached results.
- Pydantic validation reduces malformed payloads and makes the HTTP API safer to consume.
- The system expects Kafka topics and brokers to already be available during runtime.

## License

This project is currently used as an internal service and does not include a dedicated license file.
