import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_wikipedia_table(url, table_index=0):
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to retrieve the webpage")
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    
    if not tables or table_index >= len(tables):
        print("No tables found or index out of range")
        return None
    
    table = tables[table_index]
    headers = [th.text.strip() for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr")[1:]:  # Skip header row
        cells = [td.text.strip() for td in tr.find_all("td")]
        if cells:
            rows.append(cells)
    
    df = pd.DataFrame(rows, columns=headers)
    return df

# Example usage
url = "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"
df = scrape_wikipedia_table(url)
if df is not None:
    print(df.head())
