PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
STREAMLIT := $(VENV)/bin/streamlit

.PHONY: setup kafka-up kafka-down producer producer-sim stream train predict dashboard benchmark clean

setup:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

kafka-up:
	docker compose up -d

kafka-down:
	docker compose down

producer:
	$(PY) -m app.pipeline.producer

producer-sim:
	$(PY) -m app.pipeline.producer

stream:
	$(PY) -m app.pipeline.stream_processor

train:
	$(PY) -m app.pipeline.train_model

predict:
	$(PY) -m app.pipeline.stream_predictor

dashboard:
	$(STREAMLIT) run app/web/dashboard.py

benchmark:
	$(PY) -m scripts.benchmark_run

clean:
	rm -rf output/checkpoints output/bronze output/silver output/stats output/predictions output/models
