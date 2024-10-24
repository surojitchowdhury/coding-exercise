.PHONY: init test run

VENV_DIR=venv
PYTHON=$(VENV_DIR)/bin/python
PIP=$(VENV_DIR)/bin/pip
SRC_DIR=src
DB_SCRIPT=_assets/generate_duckdb_table.py
DB_FILE=db/sales.duckdb

init:
	# Create a Python virtual environment
	python3 -m venv $(VENV_DIR)
	# Activate virtual environment and install dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	# Generate the dataset (DuckDB database)
	$(PYTHON) $(DB_SCRIPT)

test:
	# Run pytest for everything in the test folder
	$(VENV_DIR)/bin/pytest tests/

run:
	# Run the Flask application
	$(PYTHON) $(SRC_DIR)/app.py

clean: 
	rm -rf $(VENV_DIR) $(DB_FILE) $(SRC_DIR)/__pycache__