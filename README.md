# Real-Time Stock Market Summary

## Overview

This project provides a real-time stock market summary pipeline using Python, Kafka, and PostgreSQL. It ingests live stock tick data, processes it into 1-minute OHLCV summaries, and stores the results in a relational database.

## Architecture 🏗️

- **Producer (Python):** Connects to the FMP WebSocket, receives real-time stock ticks, and sends them to a Kafka topic.
- **Kafka:** Acts as the streaming platform, decoupling the producer from the consumer.
- **Consumer (Python):** Reads stock ticks from Kafka, aggregates them into 1-minute summaries (Open, High, Low, Close, Volume), and saves them to a database.
- **Database (PostgreSQL):** Stores the aggregated OHLCV summaries for further analysis and reporting.

## Project Structure

```
.
├── src/
│   ├── producer.py
│   ├── consumer.py
│   └── ddl.sql
├── tests/
│   ├── test_producer.py
│   └── test_consumer.py
├── requirements.txt
├── docker-compose.yml
├── .env
└── README.md
```

## Setup Instructions

### 1. Clone the Repository

```sh
git clone https://github.com/iduryodhanrao/realtime_stockmarketsummary.git
cd realtime_stockmarketsummary
```

### 2. Create and Activate Virtual Environment

```sh
python -m venv venv
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Python Dependencies

```sh
pip install -r requirements.txt
```

### 4. Start Kafka and PostgreSQL Services

```sh
docker-compose up -d
```

### 5. Initialize the Database

```sh
# Make sure PostgreSQL is running and accessible
psql -h localhost -U postgres -f src/ddl.sql
```
*You may need to set the password for the `postgres` user. Use the credentials from your `.env` or `docker-compose.yml`.*

## Usage

### Run the Producer

```sh
python src/producer.py
```

### Run the Consumer

```sh
python src/consumer.py
```

## Testing

Run all tests using pytest:

```sh
pytest tests/
```

## Configuration

- Set environment variables in `.env` for database and Kafka connection details.
- Update `docker-compose.yml` as needed for your environment.

## Author

Indugu Rao

---

Feel free to contribute or open issues