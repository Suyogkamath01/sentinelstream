.PHONY: install lint format typecheck test check security benchmark compose-config generate-sample dashboard docs-validate research-smoke demo-offline

install:
	uv sync --group dev --group spark --group database --group api --group dashboard --group mlops --group monitoring --group security --group research --no-group ml

lint:
	uv run ruff check src tests scripts migrations

format:
	uv run ruff format src tests scripts migrations

typecheck:
	uv run mypy src/sentinelstream

test:
	uv run pytest

security:
	uv run bandit -r src -q
	uv run pip-audit

benchmark:
	uv run python scripts/benchmark.py --suite inference --iterations 100 --warmup 10

compose-config:
	docker compose config

docs-validate:
	uv run python scripts/validate_docs.py

research-smoke:
	uv run python scripts/run_research.py --input data/samples/transactions.parquet --output reports/research/smoke --models logistic_regression,rules --bootstrap-resamples 20

demo-offline:
	uv run python scripts/run_demo.py --count 100

check: lint typecheck test

generate-sample:
	uv run sentinel-generate --count 1000 --jsonl data/samples/transactions.jsonl --parquet data/samples/transactions.parquet

dashboard:
	uv run streamlit run src/sentinelstream/dashboard/app.py
