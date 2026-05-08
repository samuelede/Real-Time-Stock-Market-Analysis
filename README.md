# Real-Time Stock Market Analysis

A scalable, end-to-end data pipeline to ingest, process, and visualise stock market data in real-time. Built for **MarketPulse Analytics**, a FinTech firm serving institutional investors. This project leverages a modern data stack to handle high-velocity data streams, providing actionable insights through live dashboards.

![Data Pipeline Architecture](/img/data-pipeline-architecture.svg)

---

## Table of Contents

<ol>
  <li><a href="#project-overview" style="color: white; text-decoration: none;">Project Overview</a></li>
  <li><a href="#business-context" style="color: white; text-decoration: none;">Business Context</a></li>
  <li><a href="#pipeline-architecture" style="color: white; text-decoration: none;">Pipeline Architecture</a></li>
  <li><a href="#tech-stack" style="color: white; text-decoration: none;">Tech Stack</a></li>
  <li><a href="#prerequisites" style="color: white; text-decoration: none;">Prerequisites</a></li>
  <li><a href="#getting-started" style="color: white; text-decoration: none;">Getting Started</a>
    <ol>
      <li><a href="#step-1--create-a-rapidapi-account-and-get-your-alpha-vantage-api-key" style="color: white; text-decoration: none;">Create a RapidAPI account and get your Alpha Vantage API key</a></li>
      <li><a href="#step-2--clone-the-repository" style="color: white; text-decoration: none;">Clone the repository</a></li>
      <li><a href="#step-3--configure-environment-variables" style="color: white; text-decoration: none;">Configure environment variables</a></li>
      <li><a href="#step-4--install-python-dependencies" style="color: white; text-decoration: none;">Install Python dependencies</a></li>
      <li><a href="#step-5--test-the-api-connection" style="color: white; text-decoration: none;">Test the API connection</a></li>
    </ol>
  </li>
  <li><a href="#running-the-pipeline" style="color: white; text-decoration: none;">Running the Pipeline</a>
    <ol>
      <li><a href="#step-1--start-all-infrastructure-services" style="color: white; text-decoration: none;">Start all infrastructure services</a></li>
      <li><a href="#step-2--confirm-kafka-is-ready" style="color: white; text-decoration: none;">Confirm Kafka is ready</a></li>
      <li><a href="#step-3--start-the-stock-data-producer" style="color: white; text-decoration: none;">Start the stock data producer</a></li>
      <li><a href="#step-4--submit-the-spark-streaming-job" style="color: white; text-decoration: none;">Submit the Spark streaming job</a></li>
      <li><a href="#step-5--verify-data-in-postgresql" style="color: white; text-decoration: none;">Verify data in PostgreSQL</a></li>
    </ol>
  </li>
  <li><a href="#accessing-the-services" style="color: white; text-decoration: none;">Accessing the Services</a></li>
  <li><a href="#connecting-power-bi" style="color: white; text-decoration: none;">Connecting Power BI</a></li>
  <li><a href="#project-structure" style="color: white; text-decoration: none;">Project Structure</a></li>
  <li><a href="#environment-variables" style="color: white; text-decoration: none;">Environment Variables</a></li>
  <li><a href="#stopping-the-pipeline" style="color: white; text-decoration: none;">Stopping the Pipeline</a></li>
  <li><a href="#learning-outcomes" style="color: white; text-decoration: none;">Learning Outcomes</a></li>
  <li><a href="#contributing" style="color: white; text-decoration: none;">Contributing</a></li>
  <li><a href="#license" style="color: white; text-decoration: none;">License</a></li>
</ol>

---

## Project Overview

MarketPulse Analytics faces increasing demand for ultra-low latency financial data as data volumes grow. The current infrastructure struggles to handle peak market periods such as stock market opens and earnings reports leading to delays that affect trading decisions and client satisfaction.

This project addresses those challenges by building a fault-tolerant, real-time data pipeline that:

- **Collects** stock market data from the [Alpha Vantage API](https://rapidapi.com/alphavantage/api/alpha-vantage) via RapidAPI, providing live equity prices, trading volumes, and financial metrics
- **Streams** the collected data through Apache Kafka for real-time event processing
- **Processes** the stream using Apache Spark for transformations and analytics
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

### Step 1 — Create a RapidAPI account and get your Alpha Vantage API key

The producer fetches live stock market data from the **Alpha Vantage** API, accessed via RapidAPI.

1. Go to [https://rapidapi.com](https://rapidapi.com) and sign up for a free account using a valid email address
2. Once logged in, use the search bar to search for **Alpha Vantage**
3. From the results, select the one listed under the **Finance** category
4. On the API page, click the **Test Endpoint** tab to browse the available endpoints and confirm results are returning successfully
5. In the code snippet panel on the right, select **Python** from the language dropdown
6. Copy the generated code — this reflects the structure used in `producer/extract.py`
7. Navigate to the **Pricing** tab and subscribe to the free tier if prompted

Once you have your key, copy it from the **App** section under your RapidAPI dashboard (it appears as the `X-RapidAPI-Key` header value in the code snippet).

### Step 2 — Clone the repository

```bash
git clone https://github.com/samuelede/Real-Time-Stock-Market-Analysis.git
cd Real-Time-Stock-Market-Analysis
```

### Step 3 — Configure environment variables

Create a `.env` file in the project root:

```bash
touch .env
```

Populate it with your credentials — the API key must come first:

```env
API_KEY=your_rapidapi_key_here

POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=stock_market
```

> **Important:** Never commit your `.env` file to version control. It is listed in `.gitignore` by default.

### Step 4 — Install Python dependencies

```bash
pip install -r requirements.txt
```

### Step 5 — Test the API connection

Before starting the full pipeline, confirm the producer can successfully fetch data from Alpha Vantage:

```bash
python producer/extract.py
```

You should see stock market data printed to the terminal. If you receive an authentication error, double-check your `API_KEY` value in `.env`.

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

The producer fetches live stock market data from Alpha Vantage and publishes it as JSON events to Kafka. Run it from the project root:

```bash
python producer/main.py
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
├── consumer/
│   ├── consumer.py          # Spark streaming job — consumes from Kafka, writes to PostgreSQL
│   └── Dockerfile           # Container definition for the Spark consumer
├── img/
│   └── data-pipeline-architecture.svg   # Architecture diagram
├── producer/
│   ├── config.py            # Configuration and environment variable loading
│   ├── extract.py           # Fetches stock data from Alpha Vantage via RapidAPI
│   ├── main.py              # Entry point — orchestrates extraction and Kafka publishing
│   └── producer_setup.py   # Kafka producer initialisation and topic setup
├── compose.yml              # Docker Compose service definitions
├── requirements.txt         # Python dependencies
├── consumer.py              # Top-level consumer script (root-level entry point)
├── .env                     # Local environment variables (not committed)
├── .gitignore
└── README.md
```

---

## Environment Variables

The following variables are required in your `.env` file:

| Variable | Description | Example |
|---|---|---|
| `API_KEY` | RapidAPI key for Alpha Vantage stock data | `your_rapidapi_key_here` |
| `POSTGRES_USER` | PostgreSQL superuser name | `stockuser` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `securepassword` |
| `POSTGRES_DB` | Name of the database to create | `stock_market` |

These are injected into the relevant services at container startup via `compose.yml`.

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

## Contributing

Contributions are welcome and appreciated. If you'd like to improve the pipeline, fix a bug, or extend the project with new features, please follow the steps below.

### How to contribute

1. **Fork the repository** — click the Fork button at the top of the [GitHub page](https://github.com/samuelede/Real-Time-Stock-Market-Analysis)
2. **Create a feature branch** from `main`:
```bash
   git checkout -b feature/your-feature-name
```
3. **Make your changes** and ensure the pipeline still runs end-to-end before committing
4. **Commit with a clear message:**
```bash
   git commit -m "feat: describe what your change does"
```
5. **Push your branch:**
```bash
   git push origin feature/your-feature-name
```
6. **Open a Pull Request** against the `main` branch with a description of what you changed and why

### Guidelines

- Keep pull requests focused — one feature or fix per PR
- Follow the existing code style and naming conventions
- If adding a new service or dependency, update `compose.yml`, `requirements.txt`, and this README accordingly
- Do not commit `.env` files or any credentials

### Reporting issues

Found a bug or have a suggestion? Open an issue on the [Issues page](https://github.com/samuelede/Real-Time-Stock-Market-Analysis/issues) with as much detail as possible, including steps to reproduce if applicable.

---

## License

This project is licensed under the [MIT License](LICENSE?tab=MIT-1-ov-file)
