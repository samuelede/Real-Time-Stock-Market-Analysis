# Real-Time Stock Market Analysis

A scalable, end-to-end data pipeline to ingest, process, and visualise stock market data in real-time. Built for **MarketPulse Analytics**, a FinTech firm serving institutional investors. This project leverages a modern data stack to handle high-velocity data streams, providing actionable insights through live dashboards.

![Data Pipeline Architecture](/img/data-pipeline-architecture.svg)

---

## Table of Contents

- [Real-Time Stock Market Analysis](#real-time-stock-market-analysis)
  - [Table of Contents](#table-of-contents)
  - [Project Overview](#project-overview)
    - [Expected Outcomes](#expected-outcomes)
  - [Business Context](#business-context)
  - [Pipeline Architecture](#pipeline-architecture)
    - [Tech Stack and Data Flow](#tech-stack-and-data-flow)
  - [Prerequisites](#prerequisites)
  - [Getting Started](#getting-started)
    - [1. Clone the repository](#1-clone-the-repository)
    - [2. Configure environment variables](#2-configure-environment-variables)
    - [3. Install Python dependencies (for the producer)](#3-install-python-dependencies-for-the-producer)
  - [Running the Pipeline](#running-the-pipeline)
    - [Step 1 — Start all infrastructure services](#step-1--start-all-infrastructure-services)
    - [Step 2 — Confirm Kafka is ready](#step-2--confirm-kafka-is-ready)
    - [Step 3 — Start the stock data producer](#step-3--start-the-stock-data-producer)
    - [Step 4 — Submit the Spark streaming job](#step-4--submit-the-spark-streaming-job)
    - [Step 5 — Verify data in PostgreSQL](#step-5--verify-data-in-postgresql)
  - [Accessing the Services](#accessing-the-services)
  - [Connecting Power BI](#connecting-power-bi)
  - [Project Structure](#project-structure)
  - [Environment Variables](#environment-variables)
  - [Stopping the Pipeline](#stopping-the-pipeline)
  - [Learning Outcomes](#learning-outcomes)
  - [License](#license)

---

## Project Overview

MarketPulse Analytics faces increasing demand for ultra-low latency financial data as data volumes grow. The current infrastructure struggles to handle peak market periods — such as stock market opens and earnings reports — leading to delays that affect trading decisions and client satisfaction.

This project addresses those challenges by building a fault-tolerant, real-time data pipeline that:

- **Collects** stock market data from financial sources and streams it through Apache Kafka
- **Processes** the stream in real-time using Apache Spark for transformations and analytics
- **Stores** processed data in PostgreSQL for querying and reporting
- **Visualises** insights via Power BI dashboards, delivering real-time stock trends, trading volumes, and sentiment signals to clients

### Expected Outcomes

- **Scalable pipeline** capable of processing high-velocity streams with low latency
- **Real-time insights** via interactive dashboards for stock trends and trading volumes
- **Operational efficiency** with reduced processing delays and more reliable analytics
- **Client satisfaction** through faster, data-driven decision support

---

## Business Context

**Company:** MarketPulse Analytics  
**Location:** New York City, USA  
**Industry:** FinTech — Financial Data Analytics  
**Founded:** 2016

MarketPulse provides real-time market feeds, custom reporting dashboards, and predictive insights to hedge funds, asset managers, and electronic brokers. Key milestones include launching the first real-time reporting platform in 2016, expanding to global exchanges in 2018, and adding advanced analytics and sentiment modelling in 2022.

**Key challenges driving this project:**

- **Data latency** — delays integrating diverse sources (exchanges, news, social sentiment) reduce insight accuracy
- **Scalability** — the existing infrastructure bottlenecks under peak market load
- **System reliability** — lack of robust monitoring makes anomaly detection difficult

---

## Pipeline Architecture

```
Financial APIs  ──►  Kafka (broker)  ──►  Spark (stream processor)  ──►  PostgreSQL  ──►  Power BI
                        │
                    Kafka UI
                  (topic inspector)
```

### Tech Stack and Data Flow

| Component | Role |
|---|---|
| **API Producer** | Publishes JSON stock events to a Kafka topic |
| **Apache Kafka** | Distributed message broker; buffers and streams events in real-time |
| **Kafka UI** | Web interface to inspect topics, consumer groups, and message payloads |
| **Apache Spark** | Consumes from Kafka, applies transformations, writes results to PostgreSQL |
| **PostgreSQL** | Stores processed analytics data for querying and reporting |
| **pgAdmin** | Web-based GUI for managing and querying the PostgreSQL database |
| **Power BI** | External dashboard tool; connects directly to PostgreSQL for live reporting |

---

## Prerequisites

Ensure the following are installed on your machine before proceeding:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v24+ recommended) with Docker Compose v2
- [Git](https://git-scm.com/)
- [Python 3.9+](https://www.python.org/) (for running the producer locally)
- [Power BI Desktop](https://powerbi.microsoft.com/) (optional — for dashboard visualisation)

Verify your Docker installation:

```bash
docker --version
docker compose version
```

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/samuelede/Real-Time-Stock-Market-Analysis.git
cd Real-Time-Stock-Market-Analysis
```

### 2. Configure environment variables

Create a `.env` file in the project root. This file is used by `compose.yml` to inject secrets into the PostgreSQL container:

```bash
cp .env.example .env   # if an example file exists, otherwise create manually
```

Populate `.env` with your credentials:

```env
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=stock_market
```

> **Important:** Never commit your `.env` file to version control. It is listed in `.gitignore` by default.

### 3. Install Python dependencies (for the producer)

```bash
pip install -r requirements.txt
```

---

## Running the Pipeline

All infrastructure services are defined in `compose.yml` and run as Docker containers on a shared `stock_data` network.

### Step 1 — Start all infrastructure services

From the project root, bring up all containers in detached mode:

```bash
docker compose up -d
```

Docker will pull the required images (first run may take a few minutes) and start the following services: `spark-master`, `spark-worker`, `kafka`, `kafka-ui`, `postgres`, and `pgadmin`.

Verify all containers are running:

```bash
docker compose ps
```

You should see all services with a `running` status.

### Step 2 — Confirm Kafka is ready

Wait ~15–20 seconds for Kafka to complete its KRaft initialisation, then open Kafka UI in your browser:

```
http://localhost:8085
```

You should see a cluster named **local** with no topics yet. If the cluster does not appear, wait a few more seconds and refresh.

### Step 3 — Start the stock data producer

The producer application publishes simulated (or live) stock market events as JSON to a Kafka topic. Run it from the `producer/` directory:

```bash
cd producer
python producer.py
```

Once running, return to Kafka UI (`http://localhost:8085`) and navigate to **Topics**. You should see the stock market topic appear with incoming messages.

### Step 4 — Submit the Spark streaming job

The Spark job consumes messages from Kafka, applies transformations, and writes the results to PostgreSQL. Submit it to the Spark master container:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  /path/to/your/spark_job.py
```

> Replace `/path/to/your/spark_job.py` with the actual path to the Spark consumer script inside the container, or mount it via a volume.

Monitor the job progress at the Spark Master web UI:

```
http://localhost:8077
```

### Step 5 — Verify data in PostgreSQL

Open pgAdmin at `http://localhost:5050` and log in with:

- **Email:** `admin@admin.com`
- **Password:** `admin`

Register a new server with the following connection details:

| Field | Value |
|---|---|
| Host | `postgres` (or `localhost` if connecting from outside Docker) |
| Port | `5434` |
| Database | value from `POSTGRES_DB` in your `.env` |
| Username | value from `POSTGRES_USER` in your `.env` |
| Password | value from `POSTGRES_PASSWORD` in your `.env` |

Query the processed stock data table to confirm records are being written.

---

## Accessing the Services

| Service | URL | Credentials |
|---|---|---|
| Spark Master UI | http://localhost:8077 | — |
| Spark Worker UI | http://localhost:8081 | — |
| Kafka UI | http://localhost:8085 | — |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |
| PostgreSQL (direct) | localhost:5434 | from `.env` |

---

## Connecting Power BI

Power BI connects externally to the PostgreSQL database to build live dashboards.

1. Open **Power BI Desktop**
2. Select **Get Data → PostgreSQL database**
3. Enter the following connection details:
   - **Server:** `localhost:5434`
   - **Database:** value from `POSTGRES_DB` in your `.env`
4. Authenticate with your PostgreSQL username and password
5. Select the relevant tables and build your reports

For scheduled refresh or cloud publishing, configure the **Power BI On-premises Data Gateway**.

---

## Project Structure

```
Real-Time-Stock-Market-Analysis/
├── producer/               # Kafka producer — publishes stock events to Kafka
├── img/                    # Architecture diagrams and assets
├── compose.yml             # Docker Compose service definitions
├── requirements.txt        # Python dependencies
├── .env                    # Local environment variables (not committed)
├── .gitignore
└── README.md
```

---

## Environment Variables

The following variables are required in your `.env` file:

| Variable | Description | Example |
|---|---|---|
| `POSTGRES_USER` | PostgreSQL superuser name | `stockuser` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `securepassword` |
| `POSTGRES_DB` | Name of the database to create | `stock_market` |

These are injected into the `postgres` service at container startup via `compose.yml`.

---

## Stopping the Pipeline

To stop all running containers without removing volumes (data is preserved):

```bash
docker compose stop
```

To stop and remove all containers, networks, and volumes (full teardown):

```bash
docker compose down -v
```

---

## Learning Outcomes

Working through this project builds practical skills in:

- **Data engineering** — designing and operating real-time pipelines with Kafka, Spark, and PostgreSQL
- **Stream processing** — consuming, transforming, and writing continuous data streams at low latency
- **Containerisation** — orchestrating multi-service architectures with Docker Compose
- **Analytics and visualisation** — connecting live operational data to Power BI for client-facing reporting
- **Secrets management** — handling credentials securely via environment variables

---

## License

This project is licensed under the [MIT License](LICENSE).
