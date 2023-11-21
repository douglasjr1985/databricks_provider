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
		git diff --name-status $(GITHUB_BEFORE_COMMIT) $(GITHUB_HEAD_COMMIT_ID) | awk '{sub(/.*resource/, "resource"); print}' | grep '\.json$$' | sed 's/\r$$//' > changed-files.txt; \
	else \
		echo "No changes in this push."; \
	fi

# # Run the Python script with modified files and deploy.
deploy: 
	while IFS= read -r filename; do \
 		if [ -n "$$filename" ]; then \
 		    echo "Deploying $$filename"; \
 			$(PYTHON) main.py --workspace_url "$(DATALAKE_DATABRICKS_WORKSPACE_URL_PRD)" --client_secret "$(DATALAKE_DATABRICKS_CLIENT_SECRET)" --path_config "$$filename"; \
 		fi; \
 	done < changed-files.txt

# Default target
all: install list-modified-files deploy

 