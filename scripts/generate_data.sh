#!/bin/bash

# Paths
INPUT_FILE="/home/vincenzo/aggregate_modeling/data/tests/input.csv"

# Run python
python pyflink/generate_csv.py --output ${INPUT_FILE} --duration 300 --rate 100 --seed 42 --keys 200
