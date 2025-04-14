#!/bin/bash

# Paths
INPUT_FILE="/home/vincenzo/aggregate_modeling/data/tests/input.csv"
INJECTIONRATE_CSV="/home/vincenzo/aggregate_modeling/data/tests/injectionRate.csv"
INJECTIONRATE_PDF="/home/vincenzo/aggregate_modeling/data/tests/injectionRate.pdf"

# Run python
python pyflink/generate_csv.py --output ${INPUT_FILE} --duration 300 --rate 100 --seed 42 --keys 200 --injectionRateStat ${INJECTIONRATE_CSV}

# Create a plot too
python pyflink/create_single_csv_plot.py --csv ${INJECTIONRATE_CSV} --output ${INJECTIONRATE_PDF} --xlabel "Time (s)" --ylabel "Injection rate (\$10^3\$ t/s)" --xscale 1 --yscale 1