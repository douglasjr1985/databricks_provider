# Define variables
PYTHON := python3
PIP := pip
PYTEST := pytest

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# Run tests using pytest
#test:
#	$(PYTEST) tests

# List modified files
list-modified-files:
	if [ "$(GITHUB_HEAD_COMMIT_ID)" != "$(GITHUB_BEFORE_COMMIT)" ]; then \
		git diff --name-status $(GITHUB_BEFORE_COMMIT) $(GITHUB_HEAD_COMMIT_ID) | awk '{print $$2}' > changed-files.txt; \
	else \
		echo "No changes in this push."; \
	fi

# Run the Python script with modified files and deploy.
deploy: 
	while IFS= read -r path_config; do \
		$(PYTHON) main.py --workspace_url "$(DATALAKE_DATABRICKS_WORKSPACE_URL_PRD)" --client_secret "$(DATALAKE_DATABRICKS_CLIENT_SECRET)" --path_config "$$path_config"; \
	done < changed-files.txt

# Default target
all: install list-modified-files deploy