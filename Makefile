.PHONY: setup ais_stream pipeline flask_app cleanup

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

pipeline:
	@echo "Uruchamianie potoku Dataflow..."
	. venv/bin/activate && \
	export $(shell sed 's/#.*//g' .env | xargs) && \
	python pipeline.py \
		--runner=DataflowRunner \
		--project=$$GOOGLE_CLOUD_PROJECT \
		--region=$$REGION \
		--staging_location=$$STAGING_LOCATION \
		--temp_location=$$TEMP_LOCATION \
		--job_name=$$JOB_NAME \
		--input_subscription=$$INPUT_SUBSCRIPTION \
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
	@python delete_firestore_collections.py
	@echo "Zasoby wyczyszczone."