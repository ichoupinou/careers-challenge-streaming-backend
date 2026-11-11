# Convenience targets. Python 3.10+ stdlib only, no pip dependencies.

PYTHON ?= python3
SERVICE_URL ?= http://localhost:8080

.PHONY: help run build test clean-data load kill-test example smoke baseline burst offline adversarial

help:
	@echo "Targets:"
	@echo "  run            build (release) and run the Rust service on :8080"
	@echo "  build          release build of the Rust service"
	@echo "  test           cargo test for the Rust service"
	@echo "  clean-data     delete ./data (WAL); do this between eval scenarios"
	@echo "  load           Rust load generator vs \$$SERVICE_URL: 5k devices, 50k ev/s burst, offline replay"
	@echo "  kill-test      generator + kill -9 at 180s + restart; checks alarms and SSE gaps"
	@echo "  example        run the example stub service on :8080"
	@echo "  smoke          30s baseline run against \$$SERVICE_URL + scorecard"
	@echo "  baseline       60s baseline"
	@echo "  burst          3min run with two 10x bursts"
	@echo "  offline        2min run with 20% of devices going offline + replaying"
	@echo "  adversarial    4min run combining burst + offline + clock skew"
	@echo
	@echo "Override SERVICE_URL=... or DEVICES=... as needed."

DEVICES ?= 50
CARGO ?= cargo
MANIFEST := service/Cargo.toml

run:
	$(CARGO) run --release --manifest-path $(MANIFEST)

build:
	$(CARGO) build --release --manifest-path $(MANIFEST)

test:
	$(CARGO) test --manifest-path $(MANIFEST)

clean-data:
	rm -rf ./data

LOAD_DEVICES ?= 5000
LOAD_ARGS ?= --devices $(LOAD_DEVICES) --duration 90 --burst-rate 50000 --burst-at 15 --burst-secs 30 \
	--offline-frac 0.2 --offline-at 50 --offline-secs 25 --skew 30

load:
	$(CARGO) run --release --quiet --manifest-path $(MANIFEST) --bin loadgen -- --target $(SERVICE_URL) $(LOAD_ARGS)

kill-test:
	scripts/kill_restart.sh

example:
	$(PYTHON) example_solution/service.py

smoke:
	$(PYTHON) eval/check.py smoke --target $(SERVICE_URL) --devices $(DEVICES)

baseline:
	$(PYTHON) eval/check.py baseline --target $(SERVICE_URL) --devices $(DEVICES)

burst:
	$(PYTHON) eval/check.py burst --target $(SERVICE_URL) --devices $(DEVICES)

offline:
	$(PYTHON) eval/check.py offline --target $(SERVICE_URL) --devices $(DEVICES)

adversarial:
	$(PYTHON) eval/check.py adversarial --target $(SERVICE_URL) --devices $(DEVICES)
