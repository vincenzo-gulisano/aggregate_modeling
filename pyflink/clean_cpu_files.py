import os
import csv
import argparse

def process_thread_files(input_folder, input_csv):
    """
    Processes thread files in the input folder based on the input CSV.
    Renames files matching the IDs in the CSV to `cpu_<name>.csv` and deletes others.

    Args:
        input_folder (str): Path to the folder containing thread files.
        input_csv (str): Path to the CSV file containing `id` and `name` columns.
    """
    # Read the input CSV into a dictionary {id: name}
    id_to_name = {}
    with open(input_csv, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            id_to_name[row['id']] = row['name']

    # Process files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.startswith("thread_") and file_name.endswith(".csv"):
            # Extract the thread ID from the file name
            thread_id = file_name[len("thread_"):-len(".csv")]

            if thread_id in id_to_name:
                # Rename the file to `cpu_<name>.csv`
                new_name = f"cpu_{id_to_name[thread_id]}.csv"
                old_path = os.path.join(input_folder, file_name)
                new_path = os.path.join(input_folder, new_name)
                os.rename(old_path, new_path)
                print(f"Renamed: {file_name} -> {new_name}")
            else:
                # Delete the file if the ID is not in the CSV
                file_path = os.path.join(input_folder, file_name)
                os.remove(file_path)
                print(f"Deleted: {file_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process thread files based on an input CSV.")
    parser.add_argument("--input_folder", "-f", required=True, help="Path to the folder containing thread files.")
    parser.add_argument("--input_csv", "-c", required=True, help="Path to the input CSV file.")
    args = parser.parse_args()

    process_thread_files(args.input_folder, args.input_csv)