#!/bin/bash

# Paths
INPUT_FILE="/home/vincenzo/aggregate_modeling/data/tests/input.csv"
OUTPUT_DIR="/home/vincenzo/aggregate_modeling/data/tests/output"
FINAL_FILE="/home/vincenzo/aggregate_modeling/data/tests/output.txt"

THROUGHPUT_FILE="/home/vincenzo/aggregate_modeling/data/tests/throughput.csv"
WINDOWS_FILE="/home/vincenzo/aggregate_modeling/data/tests/windows.csv"

# Run python
python pyflink/testquery.py --input ${INPUT_FILE} --output ${OUTPUT_DIR} --throughputStat ${THROUGHPUT_FILE} --winsStat ${WINDOWS_FILE}

# The next is not the most elegant, but since the sink in PyFlink creates multiple files I am fixing this manually

# Find the first matching part file, even in nested folders like 2025-03-31--18/
PART_FILE=$(find "$OUTPUT_DIR" -type f -name "output-*.txt" | head -n 1)

# Check if the part file exists
if [ -f "$PART_FILE" ]; then
    echo "Renaming $PART_FILE to $FINAL_OUTPUT"
    mv "$PART_FILE" "$FINAL_FILE"
    echo "Moved and renamed to: $FINAL_FILE"

    # Remove the output directory if it's empty
    rm -rf "$OUTPUT_DIR" && echo "Removed empty directory: $OUTPUT_DIR"
else
    echo "Output file not found: $PART_FILE"
    exit 1
fi
