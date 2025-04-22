import json
import pandas as pd
import pycountry_convert as pc
from fuzzywuzzy import process
import pycountry
from fuzzywuzzy import process, fuzz

def preprocess_continents():
    continent_list = ["Africa", "Antarctica", "Asia", "Europe", "North America", "Oceania", "South America"]

    # Function to get rows from df that match items from continent_list
    def get_matching_rows(df, continent_list):
        matching_rows = []
        for continent in continent_list:
            # Perform fuzzy matching on the "NAME" column of df
            matches = df["NAME"].apply(lambda name: process.extractOne(continent, [name])[1] if isinstance(name, str) else 0)
            # Get rows with a high match score (e.g., above 80)
            matching_rows.extend(df[matches > 90].to_dict(orient="records"))
        # return matching_rows
        return pd.DataFrame(matching_rows)

    df = pd.read_csv('../data/DNEXR.REFERENCE.location.csv')

    df = df[df['IS_GROUP'] == False]
    data = df[(df['HIERARCHY'].str.startswith("ALL#WORLD")) & (df['HIERARCHY'].str.count("#") == 2) & (df['LEVEL_NAME']== 'REGION')]

    continents = get_matching_rows(data, continent_list)

    south_america = df[df['CODE'] == "SOAME"]
    south_america['HIERARCHY'] = "ALL#WORLD#SOAME"
    continents = pd.concat([continents, south_america], ignore_index=True)

    oceania = pd.DataFrame([{
        "NAME": "Oceania",
        "CODE": "OCE",
        "HIERARCHY": "ALL#WORLD#OCE",
        "IS_GROUP": False,
        "CHILDREN": None,
        "LEVEL_NAME": "REGION"
    }])

    continents = pd.concat([continents, oceania], ignore_index=True)

    antarctica = df[df['CODE'] == "ATA"]
    continents = pd.concat([continents, antarctica], ignore_index=True)
    continents["LEVEL_NAME"] = "CONTINENT"
    continents["NAME"] = continents["NAME"].str.lower().str.strip().str.replace("_", " ")
    continents["NAME"] = continents["NAME"].str.title()
    df = df[~df['CODE'].isin(continents['CODE'])]

    continents.to_csv('../data/results/continents.csv', index=False, encoding='utf-8')
    return continents, df


def assign_country_to_correct_continent(continents, df):
    with open('../data/processed_final_levels.json', encoding='utf-8') as f1:
        levels_reference = json.load(f1)

    df = pd.read_csv('../data/DNEXR.REFERENCE.location.csv')
    continent_list = ["Africa", "Antarctica", "Asia", "Europe", "North America", "Oceania", "South America"]

    df_countries = df[df['LEVEL_NAME'] == "COUNTRY"]

    def get_continent_from_country_name(country_name):
        try:
            country_alpha2 = pc.country_name_to_country_alpha2(country_name)
            country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
            country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
            return country_continent_name
        except KeyError:
            return "NOT FOUND"
    
    # Convert levels_reference to a dictionary with country names as keys and continent names as values
    levels_reference_dict = {entry['country']: entry['continent_name'] for entry in levels_reference}

    # Create a function to perform fuzzy matching and assign continent
    def assign_continent(df_countries, levels_reference_dict):
        continent_column = []
        for country_name in df_countries['NAME']:
            if isinstance(country_name, str):
                continent_from_lib = get_continent_from_country_name(country_name)
                if continent_from_lib in continent_list:
                    continent_column.append(get_continent_from_country_name(country_name))
                else:
                    match, score = process.extractOne(country_name, levels_reference_dict.keys())
                    if score > 80:  # Set a threshold for matching
                        continent_column.append(levels_reference_dict[match])
                    else:
                        continent_column.append("NOT FOUND")

        df_countries['CONTINENT'] = continent_column
        return df_countries

    df_countries = assign_continent(df_countries, levels_reference_dict)

    df_countries_with_continents = df_countries[df_countries['CONTINENT'] != "NOT FOUND"]
    countries_with_continents_not_found = df_countries[~df_countries['CONTINENT'].isin(continent_list)]

    df_countries_with_continents.to_csv('../data/results/df_countries_with_continents.csv', index=False, encoding='utf-8')
    countries_with_continents_not_found.to_csv('../data/results/countries_with_continents_not_found.csv', index=False, encoding='utf-8')

    return df_countries_with_continents, df, continents

def correcting_hierarchies(continents, df_countries_with_continents, df):
    # Create a mapping from continent name to hierarchy string
    continent_hierarchy_map = continents.set_index("NAME")["HIERARCHY"].to_dict()
    # Update the HIERARCHY column
    df_countries_with_continents["HIERARCHY"] = df_countries_with_continents.apply(
        lambda row: f"{continent_hierarchy_map.get(row['CONTINENT'], '')}#{row['CODE']}", axis=1
    )
    return df_countries_with_continents,continents, df

def add_parent_column(countries_with_corrected_hierarchies,continents, df):

    countries_with_continents_not_found = pd.read_csv('../data/results/countries_with_continents_not_found.csv', encoding='utf-8')

    codes_to_remove = set(continents['CODE']).union(set(countries_with_continents_not_found['CODE'])).union(set(countries_with_corrected_hierarchies['CODE']))

    df = df[~df['CODE'].isin(codes_to_remove)]

    # Get the set of valid country codes
    valid_country_codes = set(countries_with_corrected_hierarchies['CODE'])

    # Function to find first matching country code in hierarchy
    def find_country_code(hierarchy):
        if pd.isna(hierarchy):
            return None
        for part in hierarchy.split('#'):
            if part in valid_country_codes:
                return part
        return None

    # Apply function to create PARENT column
    df['PARENT'] = df['HIERARCHY'].apply(find_country_code)

    df = df[~(df['PARENT'].isnull() & ~df['CODE'].isin(['ALL', 'WORLD']))]

    # correcting the hieararchy of subregions
    for _, row in countries_with_corrected_hierarchies.iterrows():
        country_code = row["CODE"]
        corrected_country_h = row["HIERARCHY"]
        
        mask = (df["PARENT"] == country_code) & (df["LEVEL_NAME"].str.upper() != "COUNTRY")
        df.loc[mask, "HIERARCHY"] = df.loc[mask, "HIERARCHY"].apply(
            lambda h: corrected_country_h + h[h.find(country_code) + len(country_code):]
        )
    
    return df, countries_with_corrected_hierarchies, continents


def assign_level_numbers(df, countries_with_corrected_hierarchies, continents):
    df = df[df['IS_GROUP'] == False]
    df["LEVEL_NUMBER"] = None

    # For each country, assign level numbers to hierarchy under it
    for _, country in countries_with_corrected_hierarchies.iterrows():
        base_hierarchy = country["HIERARCHY"]
        base_level = base_hierarchy.count("#")

        # Filter rows that fall under this country
        mask = df["HIERARCHY"].str.startswith(base_hierarchy)

        # Assign level numbers by difference in '#' count
        df.loc[mask, "LEVEL_NUMBER"] = df.loc[mask, "HIERARCHY"].apply(
            lambda h: h.count("#") - base_level
        )

    df["LEVEL_NUMBER"] = df["LEVEL_NUMBER"].astype("Int64")
    return df, countries_with_corrected_hierarchies, continents

def merging(df, countries_with_corrected_hierarchies, continents):
    subregions = df.copy()
    countries_with_corrected_hierarchies = countries_with_corrected_hierarchies.rename(columns={"CONTINENT": "PARENT"})
    countries_with_corrected_hierarchies["LEVEL_NUMBER"] = 0

    continents["LEVEL_NUMBER"] = -1
    continents['PARENT'] = 'WORLD'

    locations_processed = pd.concat([df, countries_with_corrected_hierarchies, continents], ignore_index=True)
   
    ports = locations_processed[(locations_processed['LEVEL_NAME'] == 'PORT_TERMINAL') & (locations_processed['LEVEL_NUMBER'] != 1)]

    def correct_ports_hierarchy(ports, locations):
        # Create a mapping from country code to its hierarchy
        country_hierarchy_map = locations[locations['LEVEL_NAME'] == 'COUNTRY'].set_index('CODE')['HIERARCHY'].to_dict()
        # Update each port row
        ports = ports.copy()
        ports['HIERARCHY'] = ports.apply(
            lambda row: f"{country_hierarchy_map.get(row['PARENT'], '')}#{row['CODE']}", axis=1
        )
        ports['LEVEL_NUMBER'] = 1
        return ports
    
    ports = correct_ports_hierarchy(ports, locations_processed)

    locations_processed.update(ports)

    data_raw = pd.read_csv('../data/DNEXR.REFERENCE.location.csv')

    # Find rows in data_raw whose CODE does not exist in locations
    missing_in_locations = data_raw[~data_raw['CODE'].isin(locations_processed['CODE'])]

    return locations_processed, missing_in_locations

def adding_official_level_names(main_df):
    with open('../data/processed_final_levels.json', encoding='utf-8') as f1:
        admin_data = json.load(f1)

    # 1. Build code → country name (if needed)
    country_code_to_name = {entry['code']: entry['country'] for entry in admin_data}

    # 2. Build name → full entry lookup
    country_name_to_entry = {entry['country']: entry for entry in admin_data}

    # 3. Get all valid country names for fuzzy matching
    valid_country_names = list(country_name_to_entry.keys())


    def standardize_level(row):
        level_name = str(row['LEVEL_NAME']).lower().strip()

        # Skip rows with level names "country", "continent", "all", or "world"
        if level_name in ['country', 'continent', 'all', 'world']:
            return row['LEVEL_NAME']

        parent_code = row['PARENT']
        level_number = row['LEVEL_NUMBER']

        if pd.isna(parent_code):
            return row['LEVEL_NAME']

        parent_code_str = str(parent_code).strip()

        # Fetch the country name based on the parent code
        country_name = country_code_to_name.get(parent_code_str)

        if not country_name:
            # Fuzzy match the country name if it can't be found directly
            match = process.extractOne(parent_code_str, valid_country_names, scorer=fuzz.ratio)
            if not match or match[1] < 85:  # Use a higher threshold (e.g., 85) for fuzzy matching
                return row['LEVEL_NAME']
            country_name = match[0]

        # Get the hierarchy entry for the matched country
        entry = country_name_to_entry.get(country_name)
        if not entry:
            return row['LEVEL_NAME']

        # Select valid levels based on the row's level number
        if level_number == 1:
            valid_levels = entry.get('level_1', [])
        elif level_number == 2:
            valid_levels = [entry.get('level_2')] if entry.get('level_2') else []
        elif level_number == 3:
            valid_levels = [entry.get('level_3')] if entry.get('level_3') else []
        else:
            return row['LEVEL_NAME']  # Return the original if the level number is invalid

        # If no valid levels found, return the original LEVEL_NAME
        if not valid_levels:
            return row['LEVEL_NAME']

        # Perform fuzzy matching to the valid levels
        match = process.extractOne(level_name, valid_levels, scorer=fuzz.ratio)

        # If a match is found with a high enough score, return the matched level
        if match and match[1] >= 50:  # Only accept matches with a high score
            return match[0].upper()  # Convert the matched level to uppercase
        else:
            # If no match or poor match, return the first valid level
            return valid_levels[0].upper()
    
    main_df['OFFICIAL_LEVEL_NAME'] = main_df.apply(standardize_level, axis=1)
    main_df.to_csv('../data/results/locations_with_official_level_names.csv', index=False, encoding='utf-8')
    return main_df
