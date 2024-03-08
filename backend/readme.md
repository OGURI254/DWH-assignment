# FastAPI SQL Lineage Application

![FastAPI](https://img.shields.io/badge/FastAPI-0.68.0-blue)
![sqllineage](https://img.shields.io/badge/sqllineage-LATEST-green)

This is a FastAPI application for analyzing SQL queries and providing SQL lineage information. It allows you to list queries in a folder, analyze individual queries, and provide lineage for all queries within a specified directory.

## Features

- List SQL queries in a folder.
- Analyze an individual SQL query and provide lineage.
- Provide lineage for all queries in a specified directory.
- [Optional] Store and retrieve lineage information in a database.

## Prerequisites

Before running the application, make sure you have the following dependencies installed:

- Python (3.9+ recommended)
- FastAPI (0.68.0)
- sqllineage (latest version)



## Getting Started

Change into the project directory:

``` 
cd backend 
```

Create a virtual environment (optional but recommended):

``` 
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```
Install the project dependencies:

```
pip install -r requirements.txt
```

Start the FastAPI server:

```
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The FastAPI application will be accessible at http://localhost:8000.