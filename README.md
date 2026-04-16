# Multi-Channel YouTube Analytics ETL Pipeline

## Overview

This project demonstrates a production-style Data Engineering pipeline that simulates and analyzes performance data for multiple YouTube channels. It is designed to showcase real-world ETL practices including data generation, transformation, storage, orchestration, and visualization.

The system generates synthetic daily metrics for multiple channels and videos, processes the data into an analytics-ready format, and stores it in PostgreSQL for reporting through Power BI dashboards.

This project reflects industry-level practices such as modular design, configuration-driven pipelines, schema separation, and workflow orchestration using Apache Airflow.

---

## Objectives

* Track performance across multiple YouTube channels
* Compare channel growth and engagement
* Build ranking-based analytics dashboards
* Implement a structured ETL pipeline
* Orchestrate workflows using Airflow
* Store structured data in PostgreSQL
* Create analytics-ready datasets for BI tools

---

## Tech Stack

* Python
* PostgreSQL
* Apache Airflow
* Pandas
* SQLAlchemy
* Docker Compose
* Power BI

---

## Data Pipeline Architecture

The pipeline follows a structured ETL design:

1. Data Generation
   Synthetic multi-channel YouTube data is generated using configurable parameters.

2. Raw Layer
   Generated data is stored as CSV files and loaded into the `raw` schema in PostgreSQL.

3. Transformation Layer
   Data is cleaned, standardized, and enriched using Pandas.

4. Data Modeling
   Data is structured into:

   * Dimension tables (channels, videos)
   * Fact table (daily performance metrics)

5. Analytics Layer
   SQL views are created for:

   * Channel performance tracking
   * Ranking analysis
   * Top-performing videos

6. Visualization
   Power BI connects to the PostgreSQL mart schema for dashboard creation.

---

## Dashboards

### Executive Dashboard

* Total views, revenue, watch time
* Engagement rate trends
* Channel-wise comparison

### Channel Ranking Dashboard

* Rank by views, revenue, subscribers
* Channel comparison matrix

### Video Performance Dashboard

* Top videos by views and revenue
* Engagement metrics
* Category-level insights

---

## Key Features

* Config-driven pipeline
* Modular code structure
* Scalable ETL design
* Airflow-based orchestration
* PostgreSQL data warehouse
* Analytics-ready schema design
* Logging and testing support

---


