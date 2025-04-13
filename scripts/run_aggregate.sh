#!/bin/bash

# Paths
INPUT_FILE="/home/vincenzo/aggregate_modeling/data/tests/input.csv"
OUTPUT_DIR="/home/vincenzo/aggregate_modeling/data/tests"

# Run testquery.py in the background and get its PID
python pyflink/testquery.py --input ${INPUT_FILE} --output_folder ${OUTPUT_DIR} &
TESTQUERY_PID=$!

# Wait for a few seconds to allow the process to start
sleep 5

# Run thread_cpu_monitor.py with the dynamically retrieved PID
python pyflink/thread_cpu_monitor.py --pid ${TESTQUERY_PID} --output_dir ${OUTPUT_DIR} --interval 1