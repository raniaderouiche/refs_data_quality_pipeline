import os
import pandas as pd
import re

# Define the path to the problems folder
problems_folder = os.path.join('data', 'problems')

# List all CSV files in the problems folder
csv_files = [f for f in os.listdir(problems_folder) if f.endswith('.csv')]

# List to hold DataFrames
dfs = []

for csv_file in csv_files:
    # Extract problem name from filename (without extension)
    problem_name = os.path.splitext(csv_file)[0]
    # Read the CSV file
    df = pd.read_csv(os.path.join(problems_folder, csv_file))
    # Add the PROBLEM column
    df['PROBLEM'] = problem_name
    dfs.append(df)

# Concatenate all DataFrames
merged_df = pd.concat(dfs, ignore_index=True)

# Adding duplicates
duplicates = pd.read_csv("data/results/duplicates.csv")
duplicates['PROBLEM'] = "UniquenessConstraint - duplicate" 

merged_df = pd.concat([merged_df, duplicates], ignore_index=True)

# Adding outliers
outliers = pd.read_csv("data/results/outlier_locations.csv")
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

merged_df.to_csv("data/results/all_rows_with_problems.csv", index=False)


