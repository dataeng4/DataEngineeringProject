# Metadata-Driven Data Pipeline & UI Architecture

## Overview
A configuration-driven local data pipeline and user interface designed to simulate heavy, distributed data workflows. This architecture demonstrates an end-to-end data lifecycle: from metadata-defined schemas and automated high-throughput data generation to backend database orchestration and a full-stack frontend application.

## Architecture Components
* **The Blueprint (`master_config.yaml`):** The central nervous system of the project. Modifying this YAML file automatically dictates the database schema, data generation rules, and frontend form fields without altering the core Python code.
* **The Engine (`database.py` & `optimize.py`):** An SQLAlchemy-powered SQLite database engineered with a `pipeline_logs` orchestration table and B-Tree indexing to reduce search latency to single-digit milliseconds.
* **The Factory (`generator.py`):** A high-throughput batch ingestion script utilizing the `Faker` library. Designed to generate and insert 100,000 highly realistic records dynamically based on the YAML configuration.
* **The Interface (`app.py`):** A native Streamlit web application featuring a live database dashboard, strict Regex-validated data entry forms, and a search engine capable of exporting individual user profiles directly to formatted PDF and Microsoft Word documents.

## Prerequisites
* Windows 11
* Anaconda (Conda Package Manager)
* Git Version Control

## Installation & Setup

1. **Clone the repository and navigate to the project directory:**
   ```bash
   git clone <your-repository-url>
   cd DataEngineeringProject

Create and activate the isolated Conda environment:
    Bash

    conda create -n data_project python=3.10 -y
    conda activate data_project

    Install project dependencies:
    Bash

    pip install -r requirements.txt

Execution Lifecycle

Follow these exact steps to build the architecture from scratch:

1. Compile the Database Engine
Reads the YAML config and creates the empty database structure.
Bash

python database.py

2. Run the Data Factory (ETL)
Generates 100,000 records in batches and updates the orchestration logs.
Bash

python generator.py

3. Apply Performance Tuning
Builds B-Tree indexes across the database for lightning-fast search capabilities.
Bash

python optimize.py

4. Launch the Frontend UI
Starts the local Streamlit web server.
Bash

streamlit run app.py

Project Structure
Plaintext

DataEngineeringProject/
├── app.py                 # Streamlit frontend application
├── database.py            # SQLAlchemy models and connection logic
├── generator.py           # High-throughput fake data generator
├── master_config.yaml     # The master metadata blueprint
├── optimize.py            # B-Tree database indexing script
├── requirements.txt       # Python library dependencies
└── README.md              # Project documentation


