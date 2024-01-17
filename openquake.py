import requests
import re
import concurrent.futures  
import urllib3
import logging
from tqdm import tqdm
from urllib.parse import urljoin
from datetime import datetime
from bs4 import BeautifulSoup
from modules.io import write_json, read_json, into_parquet

URL = "https://earthquake.phivolcs.dost.gov.ph"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'}

# Send an HTTP GET request and parse it with BeautifulSoup
def parsed_request(link):
    r = requests.get(link, verify = False, headers = HEADERS)
    if r.ok:
        logging.debug("Status 200 on %s HTTP GET request", link)
        return BeautifulSoup(r.text, 'lxml')
    logging.warning("HTTP Request on %s did not went through")
    return None

# Create datetime object from the string (of different formats)
def _parse_date(str_date):
    for date_format in ('%d %b %Y - %I:%M:%S %p', '%d %b %Y - %I:%M %p', '%d %B %Y - %I:%M:%S %p', '%d %B %Y - %I:%M %p'):
        try:
            return datetime.strptime(str_date, date_format)
        except:
            pass
    logging.error("No valid date format for %s found", str_date)
    raise SystemExit("Parsing date failed")

def get_data(parsed_html):
    # Select the table to retrieve the data from
    tables = parsed_html.select('table table.MsoNormalTable')
    eq_t, intensity_t, extra_t = tables
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
    filename = f"conf/{month}.json"
    logging.info("%s earthquake records for the month of %s stored in %s", len(month_conf), month, filename)
    write_json(month_conf, filename)

    return month


# Using the items in `finished_urls`, set scraped=True for targets in the month's conf
def update_conf(filename, targets, finished_urls):
    # Change `scraped` value of every item in the list of dict
    for i in range(len(targets)):
        if targets[i]['url'] in finished_urls:
            targets[i]['scraped'] = True

    # Write the changes to month's conf
    logging.debug("%s conf updated with %s out of %s successfully scraped", filename, len(finished_urls), len(targets))
    write_json(targets, filename)

def download_month_data(month_link = URL):
    logging.basicConfig(filename = f"logs/{datetime.now().strftime('%Y %m %d - %I:%M %p')}.log", 
                        encoding="utf-8", 
                        level=logging.DEBUG,
                        format = '%(levelname)s: %(message)s'
                        )
    urllib3.disable_warnings()

    # Target month should be in the URL (in python args)
    print(f"Fetching earthquake records from {month_link}")
    html = parsed_request(month_link)
    
    # Download if the target month's JSON does not yet exist    
    month_name = download_links(html)

    # Retrieve the links from JSON file  
    month_conf =  f'conf/{month_name}.json'
    targets = read_json(month_conf)
    print(f"List of targets saved to ./conf/{month_name}.json")

    # Fetch the month's all data
    print("Now parsing each earthquake document record to retrieve the data")
    data = []  
    finished_urls = set()
    
    # Use tqdm for progress bar  
    bar = tqdm(total=len(targets), desc="Traversing each earthquake activity")
    
    # Send the requests on the earthquake record urls with multithreading
    with concurrent.futures.ThreadPoolExecutor() as executor: 
        # Return the parsed html (from the HTTP request), and the original URL (from targets)                       
        for result in executor.map(
            lambda target: {
                'url': target['url'], 
                'parsed_html': parsed_request(target['url'])
            },
            targets
            ):
            url = result['url']
            # Use the parsed html to retrieve the earthquake data
            parsed_html = result['parsed_html']
            if parsed_html:
                try:
                    parsed_data = get_data(parsed_html)
                    data.append(parsed_data)
                    # Use the original URL to keep track of successful scraping
                    finished_urls.add(url)
                except ValueError:
                    logging.warning("Parsing data from %s failed", url)
                
                    
            bar.update(n=1)    

    update_conf(month_conf, targets, finished_urls)
    
    
    # Transform and save data into parquet
    into_parquet(data, month_name)

    # Program Completion
    print(f"Process Complete. Total {len(finished_urls)}/{len(targets)} targets successfully scraped.")
    print("Data saved into test.parquet")