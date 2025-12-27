.PHONY: deps seed run test test_save docs lint_models lint_tests all

deps:
	dbt deps

seed:
	dbt seed

run:
	dbt run

test:
	dbt test

test_save:
	dbt test | tee demo_data/test_results.log

docs:
	dbt docs generate && dbt docs serve --port 8001

lint_models:
	sqlfluff lint models/

lint_tests:
	sqlfluff lint tests/

all: deps seed run test docs
