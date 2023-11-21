# Databricks Workflow

This repository contains a Databricks workflow management system. It allows you to manage Databricks jobs, configurations, and deploy new job configurations. The system uses the Databricks API to create, update, and run jobs.

## Project structure

    databricks-provider-script/
    |-- toolkit/
    | |-- init.py
    | |-- resourcecontroller.py
    | |-- databricksmanager/
    | | -- cluster.py
    | | -- instancepool.py    
    | | -- init.py
    |-- tests/
    | |-- init.py
    | |-- test_settings.py
    |-- resources/
    | |-- job_name.json
    |-- main.py
    |-- README.md
    |-- pytest.ini
    |-- .gitignore

## Table of Contents

- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Installation](#installation)


## Getting Started

Follow these instructions to get the project up and running.

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Python 3.12.0
- Pip
- Pytest
- Databricks account

## Installation

1. Clone the repository:

   ```bash
   https://github.com/dock-tech/databricks-provider-script.git
   cd databricks_workflow
   pip install -r requirements.txt

**Usage**

To use this Databricks workflow system, follow these steps:

1. Ensure you have the Databricks API token.
   
2. Create job configurations in the `resources/` folder as JSON files.

3. Run the deployment script:

   ```bash
   python3 main.py --workspace_url dominio.databricks.com --client_secret token --file_path file_path_name

## Scripts and Configuration

- main.py: Main script to create or update jobs using Databricks API.
- resources/: Folder containing job configuration JSON files.
- requirements.txt: List of Python dependencies.
- Makefile: Contains convenient commands for installation, testing, and deployment.   
 