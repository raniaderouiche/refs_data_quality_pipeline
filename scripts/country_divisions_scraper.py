import requests
from bs4 import BeautifulSoup
import pandas as pd


def scrape_wikipedia_table(table_index=0):
    url = "https://en.wikipedia.org/wiki/List_of_administrative_divisions_by_country"
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
    
    header_rows = table.find_all("tr")[:2]
    headers = []
    first_row_headers = [th.text.strip() for th in header_rows[0].find_all("th")]
    second_row_headers = [th.text.strip() for th in header_rows[1].find_all("th")]
    
    headers.extend(first_row_headers[:2]) 
    headers.extend(second_row_headers)  
    
    rows = []
    for tr in table.find_all("tr")[2:]:  # Skip header rows
        cells = [td.text.strip() for td in tr.find_all("td")]
        if cells:
            while len(cells) < len(headers):
                cells.append('')
            while len(cells) > len(headers):
                cells = cells[:len(headers)]
            rows.append(cells)
    
    df = pd.DataFrame(rows, columns=headers)
    return df