.PHONY: setup ais_stream pipeline_live pipeline_arch flask_app cleanup live arch

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

pipeline_live:
	@echo "Uruchamianie potoku Dataflow (kolizje, StatefulDoFn)..."
	. venv/bin/activate && \
	export $(shell sed 's/#.*//g' .env | xargs) && \
	python pipeline_collisions.py \
  		--runner=DataflowRunner \
  		--project=ais-collision-detection \
  		--region=us-east1 \
  		--staging_location=gs://ais-collision-detection-bucket/staging \
  		--temp_location=gs://ais-collision-detection-bucket/temp \
  		--job_name=collision-detector \
  		--input_subscription=projects/ais-collision-detection/subscriptions/ais-data-sub \
  		--collisions_topic=projects/ais-collision-detection/topics/collisions-topic \
  		--requirements_file=requirements.txt \
  		--save_main_session

pipeline_arch:
	@echo "Uruchamianie potoku Dataflow (archiwizacja AIS do GCS)..."
	. venv/bin/activate && \
	export $(shell sed 's/#.*//g' .env | xargs) && \
	python pipeline_archive.py \
  		--runner=DataflowRunner \
  		--project=ais-collision-detection \
  		--region=us-east1 \
  		--staging_location=gs://ais-collision-detection-bucket/staging \
  		--temp_location=gs://ais-collision-detection-bucket/temp \
  		--job_name=ais-archive \
  		--input_subscription=projects/ais-collision-detection/subscriptions/ais-data-sub \
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

# --- Aliasy skracające wywołania ---
live: pipeline_live
arch: pipeline_arch