#!/bin/bash
# load_bq.sh - Prosty batch load do BQ (us-east1)

bq load \
  --location=us-east1 \
  --source_format=CSV \
  --autodetect \
  ais-collision-detection:ais_dataset_us.ships_positions \
  gs://ais-collision-detection-bucket/ais_data/raw/*.csv