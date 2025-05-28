import os
import pandas as pd
import re
import glob
import shutil
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.config_loader import load_config

config = load_config()
problems_dir = config["paths"]["problems_dir"]
duplicates_path = config["paths"]["duplicates_path"]
outliers_path = config["paths"]["outliers_path"]
rows_with_problems_path = config["paths"]["rows_with_problems_path"]

def merge_problems_from_local_files():
    # List all CSV files in the problems folder
    csv_files = [f for f in os.listdir(f"../{problems_dir}") if f.endswith('.csv')]

    # List to hold DataFrames
    dfs = []

    for csv_file in csv_files:
        # Extract problem name from filename (without extension)
        problem_name = os.path.splitext(csv_file)[0]
        # Read the CSV file
        df = pd.read_csv(os.path.join(f"../{problems_dir}", csv_file))
        # Add the PROBLEM column
        df['PROBLEM'] = problem_name
        dfs.append(df)

    merged_df = pd.concat(dfs, ignore_index=True)

    return merged_df



def detect_problems(problems):

    # if problems:
    #     if os.path.exists(problems_dir):
    #         for filename in os.listdir(problems_dir):
    #             file_path = os.path.join(problems_dir, filename)
    #             try:
    #                 if os.path.isfile(file_path):
    #                     os.remove(file_path)
    #             except Exception as e:
    #                 print(f"Error deleting file {file_path}: {e}")
    all_problems = pd.DataFrame()
    for problem_name, problem_df in problems.items():
        # problem_df = problem_df.withColumn("problem_name", F.lit(problem_name))
        # problem_df = problem_df.select("NAME", "CODE", "HIERARCHY", "IS_GROUP", "CHILDREN", "LEVEL_NAME", "PARENT", "LEVEL_NUMBER", "OFFICIAL_LEVEL_NAME")
        problem_df = problem_df[["NAME", "CODE", "HIERARCHY", "IS_GROUP", "CHILDREN", "LEVEL_NAME", "PARENT", "LEVEL_NUMBER", "OFFICIAL_LEVEL_NAME"]]
        problem_df['PROBLEM'] = problem_name
        
        all_problems = pd.concat([all_problems, problem_df], ignore_index=True)
        # problem_df.coalesce(1).write.csv(f"{problems_dir}/problem_{problem_name}", header=True, mode="overwrite")
        # output_path = f"{problems_dir}/problem_{problem_name}"
        # # Construct the full path to the part file
        # part_file_path = os.path.join(output_path, "part-00000-*")
        # custom_file_name = f"{problem_name}.csv"
        # matching_files = glob.glob(part_file_path)

        # if matching_files:
        #     source_file = matching_files[0]
        #     destination_file = os.path.join(problems_dir, custom_file_name)

        #     # Check if the destination file already exists
        #     if os.path.exists(destination_file):
        #         try:
        #             os.remove(destination_file)
        #             print(f"Existing file '{destination_file}' overwritten.")
        #         except OSError as e:
        #             print(f"Error: Could not delete existing file '{destination_file}': {e}")
        #             # You might want to handle this error more gracefully, e.g., skip renaming
        #             exit(1)  # Or some other error handling
        #     try:
        #         os.rename(source_file, destination_file)
        #         print(f"Renamed '{source_file}' to '{destination_file}'")
        #     except OSError as e:
        #         print(f"Error: Could not rename file '{source_file}' to '{destination_file}': {e}")


        #     # Optionally, remove the empty folder created by Spark
        #     try:
        #         shutil.rmtree(output_path)
        #     except OSError as e:
        #         print(f"Warning: Could not remove the output directory '{output_path}'. It might not be empty: {e}")
        # else:
        #     print(f"Error: No part file found in '{output_path}'.")

def process_problems(problems, duplicates, outliers):

    merged_df = detect_problems(problems)

    # List all CSV files in the problems folder
    # csv_files = [f for f in os.listdir(problems_dir) if f.endswith('.csv')]

    # # List to hold DataFrames
    # dfs = []

    # for csv_file in csv_files:
    #     # Extract problem name from filename (without extension)
    #     problem_name = os.path.splitext(csv_file)[0]
    #     # Read the CSV file
    #     df = pd.read_csv(os.path.join(problems_dir, csv_file))
    #     # Add the PROBLEM column
    #     df['PROBLEM'] = problem_name
    #     dfs.append(df)

    # Concatenate all DataFrames
    # merged_df = pd.concat(dfs, ignore_index=True)

    # Adding duplicates
    # duplicates = pd.read_csv(duplicates_path)
    duplicates['PROBLEM'] = "UniquenessConstraint - duplicate" 

    merged_df = pd.concat([merged_df, duplicates], ignore_index=True)

    # Adding outliers
    # outliers = pd.read_csv(outliers_path)
    outliers['PROBLEM'] = "ComplianceConstraint - outlier" 

    merged_df = pd.concat([merged_df, outliers], ignore_index=True)

    types = ["NAME", "CODE", "HIERARCHY", "IS_GROUP", "CHILDREN", "LEVEL_NAME", "PARENT", "LEVEL_NUMBER", "OFFICIAL_LEVEL_NAME"]

    def extract_type(constraint):
        if str(constraint).startswith("UniquenessConstraint"):
            return "NAME"
        for t in types:
            if re.search(rf"\b{t}\b", str(constraint), re.IGNORECASE):
                return t
        return None

    merged_df["COLUMN_PROBLEM"] = merged_df["PROBLEM"].apply(extract_type)

    merged_df.to_csv(f"../{rows_with_problems_path}", index=False)
    return merged_df

if __name__ == "__main__":
    process_problems(None)


