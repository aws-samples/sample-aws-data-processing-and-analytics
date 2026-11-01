PYTHON ?= python3
STAGE ?= dev
AWS_REGION ?= us-east-1

.PHONY: install format lint test synth package-agent deploy destroy demo clean

install:
	uv sync --dev
	uv pip install -r infrastructure/requirements-dev.txt

format:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff format --check .
	uv run ruff check .

test:
	uv run pytest tests/unit

synth:
	cd infrastructure && STAGE=$(STAGE) uv run cdk synth

package-agent:
	./scripts/package_agent.sh

deploy: package-agent
	./scripts/deploy.sh $(STAGE) $(AWS_REGION)

destroy:
	./scripts/cleanup.sh $(STAGE) $(AWS_REGION)

demo:
	./scripts/run_demo.sh $(STAGE) $(AWS_REGION)

clean:
	rm -rf .build .pytest_cache .ruff_cache infrastructure/cdk.out
