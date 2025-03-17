# Project Overview

## Goal
The main goal of this project is to calculate the proportion of electric cars to the population in each city of Washington State for Q2 2023. This is achieved by implementing a **Kafka data streaming pipeline**, storing data in **Parquet format using PySpark**, creating a **MySQL database**, and performing **ratio calculations**.

## Execution Steps

### 1. Data Streaming with Kafka
- **Get Kafka Docker Container**: Pull the latest Kafka image using the command:
  ```sh
  docker pull apache/kafka:latest
  ```
- **Run Kafka Container**:
  ```sh
  docker run -d --name kafka -p 9092:9092 apache/kafka:latest
  ```

### 2. Data Processing with PySpark
- Process real-time data streaming.
- Read and write data to **Parquet format** using PySpark.

### 3. Database Storage
- Download MySQL from: [MySQL Downloads](https://www.mysql.com/downloads)
- Create a **MySQL database** to store the processed data.
- Extract necessary columns and create data tables for further calculations

### 4. Ratio Calculation
- Compute the proportion of electric cars to the population for each city in Washington State.

## Tools & Technologies Used
- **Jupyter Notebooks** (via Anaconda): [Anaconda](https://anaconda.org)
- **Docker**: [Docker Desktop](https://www.docker.com/products/docker-desktop)
- **Apache Kafka** (for data streaming)
- **PySpark** (for big data processing)
- **MySQL** (for database storage)




  

