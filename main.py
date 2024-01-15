import requests
import re
import concurrent.futures  
import urllib3
from tqdm import tqdm
from urllib.parse import urljoin
from datetime import datetime
from bs4 import BeautifulSoup
from modules.io import write_json, read_json, into_parquet

URL = "https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/2023/2023_January.html"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'}

def parsed_request(link):
    r = requests.get(link, verify = False, headers = HEADERS)
    if r.status_code == 200:
        return BeautifulSoup(r.text, 'lxml')
    return None

def _parse_date(str_date):
    for date_format in ('%d %b %Y - %I:%M:%S %p', '%d %b %Y - %I:%M %p', '%d %B %Y - %I:%M:%S %p', '%d %B %Y - %I:%M %p'):
        try:
            return datetime.strptime(str_date, date_format)
        except:
            pass
    raise ValueError(f'No valid date format for {str_date} found')

def get_data(parsed_html):
    # Select the table to retrieve the data from
    eq_t, intensity_t, extra_t = parsed_html.select('table table.MsoNormalTable')
    
    # Query for data attributes
    eq_datetime, location, depth, origin, magnitude = [i.select_one('td:nth-child(2)').text.strip() for i in eq_t.select('tr')]    
    coordinates = re.search( r'(\d+\.\d+|\d+).+, (\d+\.\d+|\d+)', location.split('-')[0])    
    damage_expected, aftershock_expected, issued_on, prepared_by = [i.select_one('td:nth-child(2)').text.strip() for i in extra_t.select('tr')]
    
    return {
        'recorded_at': _parse_date(eq_datetime),
        'depth': int(depth),
        'origin': origin.title(),
        'magnitude': float(re.search('(\d+\.\d+|\d+).+', magnitude).group()),
        'latitude': float(coordinates.group(1)),
        'longitude': float(coordinates.group(2)),
        'damage_expected': False if damage_expected.lower() == 'no' else True,
        'aftershock_expected': False if aftershock_expected.lower() == 'no' else True,
        'intensity': intensity_t.select_one('td:nth-child(2)').text.strip(),
        'issued_on': _parse_date(issued_on),
        'prepared_by': prepared_by
    }


def download_links(parsed_html):
    # Target Month
    month = parsed_html.select_one('div > table.MsoNormalTable:nth-of-type(2) strong').text

    # Data
    table = parsed_html.select_one('div > table.MsoNormalTable:nth-of-type(3)')
    records = table.select('td span a')
    month_conf = []
    # Fetch all eathquake record links
    for record in records:    
        relative_url = record['href'].replace('\\', '/')        
        earthquake_url = urljoin(URL, relative_url)    
        eq_datetime = record.text.strip()
        if earthquake_url and eq_datetime:
            month_conf.append({
                'datetime': eq_datetime,
                'url': earthquake_url,
                'scraped': False
            })    

    # Save the data into JSON file
    write_json(month_conf[:10], f"conf/{month}.json")


if __name__ == '__main__':
    urllib3.disable_warnings()

    # Target month should be in the URL (in python args)
    print("Fetching earthquake records for January 2023")
    html = parsed_request(URL)
    
    # Download if the target month's JSON does not yet exist    
    download_links(html)

    # Retrieve the links from JSON file   
    targets = read_json('conf/January 2023.json')
    print("List of targets saved to ./conf/January 2023.json")

    # Fetch the month's all data
    print("Now parsing each earthquake document record to retrieve the data")
    data = []    
    bar = tqdm(total=len(targets), desc="Traversing each earthquake activity")
    with concurrent.futures.ThreadPoolExecutor() as executor:        
        for parsed_html in executor.map(parsed_request, (target['url'] for target in targets if not target['scraped'])):
            if parsed_html:
                data.append(get_data(parsed_html))                
            bar.update(n=1)    

    # For target in targets, if target[date] in [d[date] for d in data], set target['scraped'] = True
    print(f"Process Complete. Total {len(data)}/{len(targets)} targets successfully scraped.")
    # Transform and save data into parquet
    into_parquet(data, "test")
    print("Data saved into test.parquet")