import pandas as pd
import pickle
import os
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

processed_data_path = ""

# df = pd.read_csv('data/DNEXR.REFERENCE.location.csv')

def preprocess_location_data(df):
    # Define allowed values
    allowed_values = {"COUNTRY", "CITY", "PORT_TERMINAL"}

    # Replace values not in the allowed set with "REGION"
    df["LEVEL_NAME"] = df["LEVEL_NAME"].apply(lambda x: x if x in allowed_values else "REGION")


def find_similar_locations(name, df, threshold=95):
    matches = process.extract(name, df['NAME'], limit=5)
    return [match for match in matches if match[1] >= threshold]

def get_duplicates_by_name(level_list):
    processed_indices = set()
    duplicate_values = []

    for idx, name in level_list['NAME'].items():
        if idx in processed_indices:
            continue

        similar_locations = find_similar_locations(name, level_list)
        
        # Skip if no valid duplicates found
        if len(similar_locations) <= 1:
            continue

        duplicate_values.append(similar_locations)

        print("Number of duplicates =", len(duplicate_values))

        # Add all matched indices to the set in one step
        processed_indices.update(
            level_list[level_list['NAME'].isin([loc[0] for loc in similar_locations])].index
        )
    
    return duplicate_values

def export_pkl_file(df, level_name):    
    with open(os.path.join(processed_data_path, f"duplicates_by_name_ '{level_name}'.pkl"), 'wb') as file:
        pickle.dump(df, file)

    print('data saved successfully as a pickle file.')

def fetch_rows_by_indices(df, duplicate_groups):    
    # List to maintain the order of unique indices
    unique_indices_ordered = []

    # Iterate through the match list and collect unique indices in order
    for group in duplicate_groups:
        for _, _, index in group:
            if index not in unique_indices_ordered:
                unique_indices_ordered.append(index)

    # Create a new DataFrame from the original using the ordered unique indices
    duplicate_dataframe = df.loc[unique_indices_ordered].copy()

    return duplicate_dataframe

def find_similar_groups(df, threshold_name=95, threshold_hierarchy=95):
    processed_indices = set()  # Track processed rows
    grouped_duplicates = []

    for i, row_i in df.iterrows():
        if i in processed_indices:
            continue

        similar_group = [i]  # Start a group with the current row

        for j, row_j in df.iterrows():
            if j <= i or j in processed_indices:
                continue

            # Compare NAME and HIERARCHY columns
            name_similarity = fuzz.ratio(row_i['NAME'], row_j['NAME'])
            hierarchy_similarity = fuzz.ratio(row_i['HIERARCHY'], row_j['HIERARCHY'])

            if name_similarity >= threshold_name and hierarchy_similarity >= threshold_hierarchy:
                similar_group.append(j)
                processed_indices.add(j)  # Mark j as processed

        # Add group if there are multiple matches
        if len(similar_group) > 1:
            grouped_duplicates.append(similar_group)
            processed_indices.update(similar_group)  # Ensure all are skipped later

    return grouped_duplicates


def get_rows(df, duplicates_list):
    # Extract the indices from the duplicates list
    duplicate_indices = [index for group in duplicates_list for index in group]

    # Create a new DataFrame with the duplicate rows
    duplicate_rows_df = df.loc[duplicate_indices]

    return duplicate_rows_df

def strip_hierarchy_levels(hierarchy: str, num_levels: int) -> str:
    if num_levels <= 0:
        return hierarchy
        
    levels = hierarchy.split('#')
    # Return the modified hierarchy after stripping the required levels
    return '#'.join(levels[:-num_levels]) if num_levels <= len(levels) else ''

def find_similar_groups_with_hierarchy_level(df,level_stripped, threshold_name=90, threshold_hierarchy=90):
    processed_indices = set()  # Track processed rows
    grouped_duplicates = []

    for i, row_i in df.iterrows():
        if i in processed_indices:
            continue

        similar_group = [i]  # Start a group with the current row

        for j, row_j in df.iterrows():
            if j <= i or j in processed_indices:
                continue

            # Compare NAME and HIERARCHY columns
            name_similarity = fuzz.ratio(row_i['NAME'], row_j['NAME'])
            hierarchy_similarity = fuzz.ratio(strip_hierarchy_levels(row_i['HIERARCHY'],level_stripped), strip_hierarchy_levels(row_j['HIERARCHY'],level_stripped))

            if name_similarity >= threshold_name and hierarchy_similarity >= threshold_hierarchy:
                similar_group.append(j)
                processed_indices.add(j)  # Mark j as processed

        # Add group if there are multiple matches
        if len(similar_group) > 1:
            grouped_duplicates.append(similar_group)
            processed_indices.update(similar_group)  # Ensure all are skipped later

    return grouped_duplicates

def name_duplicates_detection(df):
    levels = df['LEVEL_NAME'].dropna().unique().tolist()

    columns = df.columns.tolist()

    df_all_duplicates_by_name = pd.DataFrame(columns=columns)

    for level in levels:
        filtered_locations_by_level = df[df['LEVEL_NAME'] == level]

        duplicate_groups_by_name = get_duplicates_by_name(filtered_locations_by_level) # Find duplicates by name
        df_duplicates_by_name = fetch_rows_by_indices(df, duplicate_groups_by_name) # Get duplicate rows

        # concatenate full duplicates dataframe
        df_all_duplicates_by_name = pd.concat([df_all_duplicates_by_name, df_duplicates_by_name]).drop_duplicates(subset=columns).reset_index(drop=True)  

    return df_all_duplicates_by_name
    # merged_df_with_levels.to_csv(os.path.join(processed_data_path, 'NEW_duplicated_rows.csv'), index=False)

    # df_all_duplicates_by_name.to_csv(os.path.join(processed_data_path, 'NEW_all_duplicated_rows_by_name.csv'), index=False)
