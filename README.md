# Radiation Tracking Project (Topic C) - BD25_Project_A10_C



## Overview

This project is part of the Big Data Lab Exercises at the Institute for Data Engineering. The objective is to develop a stream processing pipeline using **Apache Kafka** and **Apache Flink** to process the **Safecast Radiation Measurements dataset**, which contains millions of radiation readings worldwide (approximately 29 GB uncompressed). The pipeline processes real-time radiation data, filters out noise, and visualizes it on a web-based map with user-configurable features, such as radiation threshold alerts, data filtering by location or time, and dynamic map updates. The solution is containerized using **Docker** and stores processed data in **MongoDB** for persistence.

Key components:
- **Apache Kafka**: Streams radiation data from the Safecast dataset in timestamp order.
- **Apache Flink**: Processes the data stream for filtering, aggregation, and alert generation.
- **MongoDB**: Stores processed radiation data for persistence.
- **Web GUI (Streamlit)**: Displays a world map with radiation data points, color-coded for safe/dangerous levels, and supports user-configurable settings.
- **Docker**: Packages the solution for seamless deployment.

## Prerequisites

- **Docker** and **Docker Compose** installed on your system.
- **Git** installed to clone the repository.
- Access to the **Safecast Radiation Measurements dataset** (download from [https://safecast.org/data/download/](https://safecast.org/data/download/)).
- A **TUHH GitLab account** for repository access.

## Architecture Diagram

The following diagram illustrates the data pipeline:

![Architecture Diagram](data/architecture_diagram.png)

- **Data Flow**: The Kafka producer reads the Safecast dataset and sends radiation measurements to Kafka topic radiation-data in timestamp order. Flink processes the stream, filters out invalid data, aggregates measurements, and generates alerts. Processed data is stored in MongoDB and visualized on a Streamlit-based GUI.

## Repository Structure

```
├── docker-compose.yml             # Defines and configures all Docker services (Flink, Kafka, MongoDB, etc.)
├── docker-compose-dockerhub.txt   # Docker Compose file for DockerHub build
├── cloud_url_A10.txt              # URL or credentials for cloud-related components
├── README.md                      # Project documentation, setup instructions, and usage guide
├── .gitignore                     # Specifies files and folders to ignore in version control
├── data/
│   ├── measurements-out.csv       # Unprocessed Radiation Dataset 
│   └── architecture_diagram.png   # Architecture diagram of the system
├── data_provider/
│   ├── Dockerfile                 # Builds the data provider service (Kafka producer container)
│   ├── producer.py                # Reads radiation data from files and sends it to Kafka
│   └── requirements.txt           # Python dependencies required for the producer
├── data_consumer/
│   ├── Dockerfile                 # Builds the Flink job container with necessary dependencies
│   ├── radiation_processor.py     # Main Flink job that processes radiation data from Kafka
│   └── requirements.txt           # Python dependencies required for the Flink job
├── mongo_uploader/
│   ├── Dockerfile                 # Builds the container with necessary dependencies
│   ├── mongo_upload.py            # Script to upload processed results from Flink to MongoDB
│   └── requirements.txt           # Python dependencies required for the Mongo Upload of filtered radiation data
├── radiation-visualizer/
│   ├── Dockerfile                 # Builds the container with necessary dependencies
│   ├── app.py                     # Pulls the filtered radiation data from MongoDB and displays it on the World map
│   ├── mongo_queries.py           # Contains MongoDB query logic for fetching/aggregating data
│   └── requirements.txt           # Python dependencies required for the Streamlit Web Application

```

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone https://collaborating.tuhh.de/e-19/teaching/bd25_project_a10_c.git
   cd bd25_project_a10_c
   ```

2. **Download Safecast Dataset**
   - Download the dataset from [https://safecast.org/data/download/](https://safecast.org/data/download/).
   - Place the dataset file (e.g., `safecast.csv`) in the `data/` directory.
   - Update the `data_provider/producer.py` and `data_provider/Dockerfile` script to point to the correct file path if necessary.

3. **Build Docker Containers**
   ```bash
   docker-compose build
   ```
   This command builds all services:
   - **Zookeeper** and **Kafka**: Manages and streams radiation data.
   - **Flink (JobManager & TaskManager)**: Processes the Kafka stream.
   - **MongoDB**: Stores processed data.
   - **Streamlit GUI**: Hosts the web-based map and configuration interface (accessible at `http://localhost:8501`).

4. **Run Docker Containers**
   ```bash
   docker-compose up -d
   ```
   This command builds and starts all services:
   - **Zookeeper** and **Kafka**: Manages and streams radiation data.
   - **Flink (JobManager & TaskManager)**: Processes the Kafka stream.
   - **MongoDB**: Stores processed data.
   - **Streamlit GUI**: Hosts the web-based map and configuration interface (accessible at `http://localhost:8501`).

5. **Verify Services**
   - Check container status: `docker-compose ps`.
   - Access the Streamlit GUI at `http://localhost:8501` to view the map and configure settings.
   - Monitor Flink job status via the Flink Web UI at `http://localhost:8081`.

6. **Stop the Pipeline**
   ```bash
   docker-compose down
   ```

## Project Flow

The pipeline operates automatically once `docker-compose up` is executed. Below is an overview of how the components interact in the backend:

1. **Kafka Producer (`data_provider/producer.py`)**
   - Reads the Safecast dataset (CSV) and sends radiation measurements (latitude, longitude, CPM, timestamp) to a Kafka topic (`radiation_data`).
   - Discards empty or invalid time data entries before sending to data_consumer.

2. **Apache Kafka**
   - Acts as a message broker, receiving data from the producer and streaming it to Flink.
   - Uses a single topic (`radiation_data`) for raw data and additional topics for processed outputs (e.g., `alerts`, `aggregated_data`).

3. **Apache Flink (`data_consumer/radiation_processor.py`)**
   - Consumes the Kafka stream using Flink’s Kafka Connector.
   - Implements functions like discarding empty or noisy data (e.g., missing CPM values), etc.
   - Outputs processed data to Kafka topic `filtered-radiation-data` and forwards it to MongoDB via a sink.

4. **MongoDB (`mongo_uploader/mongo_upload.py`)**
   - Stores processed data (e.g., filtered measurements, alerts) in a MongoDB collection (`radiation_measurements`).
   - Data is indexed by location and timestamp for efficient querying by the GUI.

5. **Streamlit GUI (`radiation-visualizer/app.py`)**
   - Retrieves data from MongoDB and displays it on a world map.
   - Supports user-configurable settings:
     - Radiation threshold (e.g., 100 or 300 CPM).
     - Time range filtering (e.g., Year-wise).

## Presentation Details

•⁠  ⁠*URL*: [BD25_Project_A10_C](https://docs.google.com/presentation/d/18Wdl_0vb-wd9Qubk8mTOOvIkPAZfVIWn5XTRhf759RM/edit?usp=sharing)

## Cloud Deployment

### Deployment URL
- The solution is deployed on Azure cloud infrastructure.
- Access URL: `http://4.204.42.195:8501/`


## DockerHub Details

### Repository URLs

- [rohant12/bd25_project_a10_c-bitnami-zookeeper](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-bitnami-zookeeper)  
- [rohant12/bd25_project_a10_c-confluentinc-cp-kafka](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-confluentinc-cp-kafka)  
- [rohant12/bd25_project_a10_c-producer](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-producer)  
- [rohant12/bd25_project_a10_c-flink-jobmanager](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-flink-jobmanager)  
- [rohant12/bd25_project_a10_c-flink-taskmanager](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-flink-taskmanager)  
- [rohant12/bd25_project_a10_c-flink-job-submitter](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-flink-job-submitter)  
- [rohant12/bd25_project_a10_c-mongo](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-mongo)  
- [rohant12/bd25_project_a10_c-mongo-uploader](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-mongo-uploader)  
- [rohant12/bd25_project_a10_c-streamlit](https://hub.docker.com/repository/docker/rohant12/bd25_project_a10_c-streamlit)

### Image Names

- rohant12/bd25_project_a10_c-bitnami-zookeeper ⁠
- rohant12/bd25_project_a10_c-confluentinc-cp-kafka ⁠
- rohant12/bd25_project_a10_c-producer ⁠
- rohant12/bd25_project_a10_c-flink-jobmanager ⁠
- rohant12/bd25_project_a10_c-flink-taskmanager ⁠
- rohant12/bd25_project_a10_c-flink-job-submitter ⁠
- rohant12/bd25_project_a10_c-mongo ⁠
- rohant12/bd25_project_a10_c-mongo-uploader ⁠
- rohant12/bd25_project_a10_c-streamlit ⁠

⁠*Access*: The Docker images are public on DockerHub. Follow the steps below to set up the project.

### Steps to Access the DockerHub Images (Windows)

1. **Log in to Docker Hub**  
   Open a terminal and log in to Docker Hub with your credentials:
   ```bash
   docker login
   ```
2. Create a working directory
   ```bash
   mkdir D:\Radiation_Analysis
   cd D:\Radiation_Analysis
   ```
3. Pull Docker images
   ```bash
   docker pull rohant12/bd25_project_a10_c-bitnami-zookeeper:latest
   docker pull rohant12/bd25_project_a10_c-confluentinc-cp-kafka:7.5.0
   docker pull rohant12/bd25_project_a10_c-producer:latest
   docker pull rohant12/bd25_project_a10_c-flink-jobmanager:latest
   docker pull rohant12/bd25_project_a10_c-flink-taskmanager:latest
   docker pull rohant12/bd25_project_a10_c-flink-job-submitter:latest
   docker pull rohant12/bd25_project_a10_c-mongo:latest
   docker pull rohant12/bd25_project_a10_c-mongo-uploader:latest
   docker pull rohant12/bd25_project_a10_c-streamlit:latest
   ```
4. Create a data subdirectory and copy the required CSV file
   ```bash
   mkdir D:\Radiation_Analysis\data
   copy D:\path\to\measurements-out.csv D:\Radiation_Analysis\data\
   ```
5. Rename docker-compose-dockerhub.txt to docker-compose.yml and move it to the working directory
   ```bash
   mv docker-compose-dockerhub.txt docker-compose.yml
   copy D:\path\to\docker-compose.yml D:\Radiation_Analysis\
   ```
6. Execute the setup
   ```bash
   cd D:\Radiation_Analysis
   docker-compose up -d
   ```
7. Verify the setup
   ```bash
   docker-compose ps
   ```
8. Access the Streamlit dashboard
   Open a browser and go to: http://localhost:8501

### Steps to Access the DockerHub Images (MacOS/Linux)

1. **Log in to Docker Hub**  
   Open a terminal and log in to Docker Hub with your credentials:
   ```bash
   docker login
   ```
2. Create a working directory
   ```bash
   mkdir -p ~/Radiation_Analysis
   cd ~/Radiation_Analysis
   ```
3. Pull Docker images
   ```bash
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-bitnami-zookeeper:latest
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-confluentinc-cp-kafka:7.5.0
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-producer:latest
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-flink-jobmanager:latest
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-flink-taskmanager:latest
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-flink-job-submitter:latest
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-mongo:latest
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-mongo-uploader:latest
   docker pull --platform=linux/arm64 rohant12/bd25_project_a10_c-streamlit:latest
   ```
4. Create a data subdirectory and copy the required CSV file
   ```bash
   mkdir -p ~/Radiation_Analysis/data
   cp /path/to/measurements-out.csv ~/Radiation_Analysis/data/
   ```
5. Rename docker-compose-dockerhub.txt to docker-compose.yml and move it to the working directory
   ```bash
   mv docker-compose-dockerhub.txt docker-compose.yml
   cp /path/to/docker-compose.yml ~/Radiation_Analysis/
   ```
6. Execute the setup
   ```bash
   cd ~/Radiation_Analysis
   docker-compose up -d
   ```
7. Verify the setup
   ```bash
   docker-compose ps
   ```
8. Access the Streamlit dashboard
   Open a browser and go to: http://localhost:8501


