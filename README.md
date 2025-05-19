[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)

# Exemple of Unit Testing using pytest for PostgreSQL

This repository provides examples of unit testing using pytest for PostgreSQL.

It is recommended to create a dedicated python virtual environment for executing this test.

## Pre requisites

To execute the pytest, docker desktop is required.

### Python virtual environment creation

```bash
mkdir venv
python3.12 -m venv venv/venv_nld_data_pipeline_pytest_postgresql
source venv/venv_nld_data_pipeline_pytest_postgresql/bin/activate
pip install -r dev-requirements.txt
pip install -r requirements.txt
pip install include/nexuslab_data_postgresql-0.0.1-py3-none-any.whl
```


## Execute the unit tests

### Test a simple pipeline (DataFrame)
```
pytest tests/data_pipelines/simple_data_pipeline/simple_data_pipeline_test.py
```

### Test a simple pipeline (SQL Query)
```
pytest tests/data_pipelines/simple_data_pipeline_sql_query/simple_data_pipeline_sql_query_test.py
```

### Test all
```
pytest
```