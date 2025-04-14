import os
import csv
import argparse
from glob import glob
import shutil

def merge_and_sort_csv(input_folder, output_csv):
    """
    Reads all CSV files in the input folder recursively, merges and sorts the data,
    and writes it to the output CSV file. Deletes the input folder afterward.

    Args:
        input_folder (str): Path to the input folder containing CSV files.
        output_csv (str): Path to the output CSV file.
    """
    all_data = []

    # Recursively find all CSV files in the folder
    csv_files = glob(os.path.join(input_folder, '**', '*.txt'), recursive=True)



    # Read all CSV files
    for csv_file in csv_files:
        with open(csv_file, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                # Ensure the row has exactly three columns
                if len(row) == 3:
                    timestamp, key, value = row
                    all_data.append((int(timestamp), int(key), float(value)))

    # Sort the data by timestamp, then by key
    all_data.sort(key=lambda x: (x[0], x[1]))

    # Write the sorted data to the output CSV
    with open(output_csv, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(all_data)

    # Remove the input folder and its contents
    shutil.rmtree(input_folder)
    print(f"Input folder '{input_folder}' and its contents have been removed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge, sort, and clean up CSV files.")
    parser.add_argument("--input_folder", "-i", required=True, help="Path to the input folder containing CSV files.")
    parser.add_argument("--output_csv", "-o", required=True, help="Path to the output CSV file.")
    args = parser.parse_args()

    merge_and_sort_csv(args.input_folder, args.output_csv)