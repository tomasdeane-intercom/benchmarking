.DEFAULT_GOAL := help

COMPOSE ?= docker compose
UV ?= uv

.PHONY: help setup run calibrate stub-calibration verify report smoke test logs clean reset purge

help:
	@printf "%s\n" \
		"Supported commands:" \
		"  make setup  - validate Docker and Compose access" \
		"  make run    - run the full benchmark suite" \
		"  make calibrate - run stub calibration" \
		"  make verify - verify stored benchmark results" \
		"  make report - generate a report from stored benchmark results" \
		"  make smoke  - run the fast smoke benchmark" \
		"  make test   - run pytest" \
		"  make logs   - tail benchmark logs" \
		"  make clean  - stop benchmark containers" \
		"  make reset  - remove benchmark containers and volumes" \
		"  make purge  - remove containers, volumes, images, and benchmark artifacts"

setup:
	$(UV) run python -m benchkit.cli.workflow setup

run:
	@$(UV) run python -m benchkit.cli.workflow group all

calibrate stub-calibration:
	$(UV) run python -m benchkit.cli.workflow stub-calibration --output-dir results/calibration

verify:
	$(UV) run python -m benchkit.cli.workflow verify --dir results/final

report:
	$(UV) run python -m benchkit.cli.workflow report --results-dir results/final --output-dir results/report

smoke:
	$(UV) run python -m benchkit.cli.workflow smoke

test:
	$(UV) run --extra test pytest

logs:
	$(UV) run python -m benchkit.cli.workflow logs

clean:
	$(UV) run python -m benchkit.cli.workflow clean

reset:
	$(UV) run python -m benchkit.cli.workflow reset

purge:
	$(UV) run python -m benchkit.cli.workflow purge
