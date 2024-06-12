.DEFAULT_GOAL := help
PY_SOURCE ?= ingestion/src
include ingestion/Makefile

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":"}; {printf "\033[35m%-35s\033[0m %s\n", $$2, $$3}'

.PHONY: py_antlr
py_antlr:  ## Generate the Python code for parsing FQNs
	antlr4 -Dlanguage=Python3 -o ingestion/src/metadata/generated/antlr ${PWD}/spec/src/main/antlr4/org/openmetadata/schema/*.g4

## Ingestion models generation
.PHONY: generate
generate:  ## Generate the pydantic models from the JSON Schemas to the ingestion module
	@echo "Running Datamodel Code Generator"
	rm -rf ingestion/src/metadata/generated
	mkdir -p ingestion/src/metadata/generated
	python scripts/datamodel_generation.py
	$(MAKE) py_antlr 

.PHONY: install_antlr_cli
install_antlr_cli:  ## Install antlr CLI locally
	echo '#!/usr/bin/java -jar' > /usr/local/bin/antlr4
	curl https://www.antlr.org/download/antlr-4.9.2-complete.jar >> /usr/local/bin/antlr4
	chmod 755 /usr/local/bin/antlr4

.PHONY: generate-schema-docs
generate-schema-docs:  ## Generates markdown files for documenting the JSON Schemas
	@echo "Generating Schema docs"
# Installing "0.4.0" version for simpler formatting
	python3 -m pip install "jsonschema2md==0.4.0"
	python3 scripts/generate_docs_schemas.py

#Upgrade release automation scripts below
.PHONY: update_all
update_all:  ## To update all the release related files run make update_all RELEASE_VERSION=2.2.2
	@echo "The release version is: $(RELEASE_VERSION)" ; \
	$(MAKE) update_pyproject_version ; \
	$(MAKE) update_dockerfile_version ; \
#remove comment and use the below section when want to use this sub module "update_all" independently to update github actions
#make update_all RELEASE_VERSION=2.2.2


.PHONY: update_pyproject_version
update_pyproject_version:  ## To update the pyproject.toml files
	file_paths="ingestion/pyproject.toml \
				airflow-apis/pyproject.toml"; \
	echo "Updating pyproject.toml versions to $(RELEASE_VERSION)... "; \
	for file_path in $$file_paths; do \
	    python3 scripts/update_version.py update_pyproject_version -f $$file_path -v $(RELEASE_VERSION) ; \
	done
# Commented section for independent usage of the module update_pyproject_version independently to update github actions
#make update_pyproject_version RELEASE_VERSION=2.2.2
