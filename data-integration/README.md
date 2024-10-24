# Data Integration Environment
This environment is intended to be used for data engineering and analytic use cases.
The attached Makefile can create the following resources for you.
- Python3 virtual environment (it will be called venv)
- DuckDB database that will mimic a sales table (with fake data)
- Libraries to interact with the db (Feel fre to refactor/alter/drop at your convenience)

## Requirements
- `make` command installed on the local machine
- A python3 version 

## Getting started
### tldr;
- `make init` will install the virtual environment and the database
- `make clean` will delete the packages and the database
- `make test` will execute every test in the `tests` folder via Pytest
- `make run` will start the `src/app.py` script

### Initialising the Environment
- `make init` will first generate a virtual environment "venv" in the project directory and will install a basic set of libraries via pip. You can alter or disregard the provided libraries at will (refer to `requirements.txt`).
- Then it will create a [DuckDB](https://duckdb.org/docs/api/python/overview.html) local database in the project subfolder `db/`.

### Running tests
`make test` will run any tests in your test folder though pytest. 

If you decide to use a different folder or test suite, it is up to you adapt the Makefile for the purpose

### Running the application
`make run` will run the src/app.py file. You are completely free to alter this behaviour at your convenience.
