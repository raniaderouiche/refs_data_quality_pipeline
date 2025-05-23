import pandas as pd
import os
from datetime import datetime
import re

df = pd.read_csv('data/results/deequ_verif_result.csv')
check_history_path = 'data/results/check_history.csv'

# Read or create check history
if os.path.exists(check_history_path):
    check_history = pd.read_csv(check_history_path)
    # Convert date column to datetime for comparison
    check_history['date'] = pd.to_datetime(check_history['date'])
else:
    check_history = pd.DataFrame(columns=['date', 'total_score'])

total_checks = len(df)
successful_checks = (df['constraint_status'] == 'Success').sum()
data_quality_score = (successful_checks / total_checks) * 100

modified_timestamp = os.path.getmtime('data/results/deequ_verif_result.csv')
formatted_date = datetime.fromtimestamp(modified_timestamp).strftime('%Y-%m-%d %H:%M:00')

print(f"Last Modified: {formatted_date}")

# Check if the date already exists in the check_history
# Convert existing dates to same string format for comparison
existing_dates = check_history['date'].dt.strftime('%Y-%m-%d %H:%M:00').tolist()


if formatted_date in existing_dates:
    print("Entry for this date already exists. Skipping append.")
else:
    # Append new row
    check_history = pd.concat([
        check_history,
        pd.DataFrame({'date': [formatted_date],
                      'total_score': [data_quality_score]})
    ], ignore_index=True)

    # Save to CSV
    check_history.to_csv(check_history_path, index=False)

types = ["NAME", "CODE", "HIERARCHY", "IS_GROUP", "CHILDREN", "LEVEL_NAME", "PARENT", "LEVEL_NUMBER", "OFFICIAL_LEVEL_NAME"]

def extract_type(constraint):
    for t in types:
        if re.search(rf"\b{t}\b", str(constraint)):
            return t
    return None

df["column_name"] = df["constraint"].apply(extract_type)
df.to_csv("data/results/deequ_verif_result_locations.csv", index=False)