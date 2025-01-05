.PHONY: setup ais_stream pipeline_live pipeline_arch flask_app cleanup live arch history pipeline_history

SHELL := /bin/bash

setup:
	@echo "Instalacja zależności..."
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip
	. venv/bin/activate && pip install -r requirements.txt

ais_stream:
	@echo "Uruchamianie strumienia AIS..."
	. venv/bin/activate && \
	python ais_stream.py

live: pipeline_live

pipeline_live:
	@echo "Uruchamianie potoku Dataflow (LIVE, collisions) ..."
	. venv/bin/activate && \
	export $(shell sed '/^ *#/d; /^$$/d' .env | xargs) && \
	python pipeline_live.py \
		--runner=DataflowRunner \
		--project=$$GOOGLE_CLOUD_PROJECT \
		--region=$$REGION \
		--staging_location=$$STAGING_LOCATION \
		--temp_location=$$TEMP_LOCATION \
		--job_name=collision-detector \
		--requirements_file=requirements.txt \
		--save_main_session

history: pipeline_history

pipeline_history:
	@echo "Uruchamianie batchowego potoku Dataflow (history collisions)..."
	. venv/bin/activate && \
	export $(shell sed '/^ *#/d; /^$$/d' .env | xargs) && \
	python pipeline_history.py \
		--runner=DataflowRunner \
		--project=$$GOOGLE_CLOUD_PROJECT \
		--region=$$REGION \
		--staging_location=$$STAGING_LOCATION \
		--temp_location=$$TEMP_LOCATION \
		--job_name=ais-history-batch \
		--requirements_file=requirements.txt \
		--save_main_session

flask_app:
	@echo "Uruchamianie aplikacji Flask..."
	. venv/bin/activate && \
	export $(shell sed '/^ *#/d; /^$$/d' .env | xargs) && \
	python app.py

cleanup:
	@echo "Czyszczenie zasobów..."
	@gcloud dataflow jobs cancel $$(gcloud dataflow jobs list --region $$(grep REGION .env | cut -d '=' -f2) --filter="NAME:$$(grep JOB_NAME .env | cut -d '=' -f2)" --format="value(JOB_ID)" | head -n 1)
	@gcloud pubsub subscriptions seek $$(basename $$(grep INPUT_SUBSCRIPTION .env | cut -d '=' -f2)) --time=+0s
	@echo "Zasoby wyczyszczone."