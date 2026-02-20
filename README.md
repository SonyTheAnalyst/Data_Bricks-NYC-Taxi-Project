# FRENCH AND ENGLISH VERSION AVAILABLE
## 🚖 Overview
This project implements an end‑to‑end data engineering pipeline for the NYC Yellow Taxi dataset, following a modern medallion‑style architecture: Landing → Bronze → Silver → Gold.
The pipeline automates ingestion, cleaning, transformation, and enrichment of taxi trip data and lookup tables using notebooks and orchestrated workflows.
The goal is to build a clean, reliable, analytics‑ready dataset for downstream BI, ML, or real‑time applications.

## 🏗️ Architecture
The workflow follows a multi‑layered data architecture:

### **1. Landing Layer**
Raw ingestion of source files.
- ingest_yellow_trip_landing
Loads raw Yellow Taxi trip data into the landing zone.
- 00_ingest_lookup_landing
Loads lookup tables (e.g., taxi zones).

### **2. Bronze Layer**
Stores raw but structured data.
- Bronze_yellow_trips_raw
Converts landing data into a structured raw format.

### **3. Silver Layer**
Cleansed and standardized datasets.
- Sliver_taxi_zone_lookup
Cleans and standardizes the taxi zone lookup table.
- Sliver_yellow_trips_cleansed
Cleans trip data (schema alignment, null handling, type casting, etc.).

### **4. Gold Layer**
Business‑ready, enriched datasets.
- Gold_yellow_trips_enriched
Combines cleansed trips with lookup tables to produce analytics‑ready data.

## 🔄 Pipeline Flow
The workflow orchestrates the following sequence:
- Ingest raw data (trips + lookup tables)
- Validate and conditionally continue downstream
- Build Bronze structured tables
- Clean and standardize into Silver
- Enrich and join into Gold
- Produce final datasets ready for dashboards or ML
All steps in the screenshot have successfully executed.

## 🧰 Technologies Used
- Data Lakehouse architecture
- Notebooks for ingestion and transformation
- Orchestrated pipelines
- Multi‑layer data modeling (Bronze/Silver/Gold)

## 🎯 Objectives
- Automate ingestion of NYC Taxi data
- Build a clean, reliable data pipeline
- Apply best practices in data engineering
- Enable analytics and machine learning use cases
