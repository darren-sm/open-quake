import os
import requests
import re
import concurrent.futures  
import urllib3
import logging
from glob import glob
from google.cloud import storage
from tqdm import tqdm
from urllib.parse import urljoin
from datetime import datetime
from bs4 import BeautifulSoup
from modules.io import *

URL = "https://earthquake.phivolcs.dost.gov.ph"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'}

# Send an HTTP GET request and parse it with BeautifulSoup
def parsed_request(link):
    try:
        r = requests.get(link, verify = False, headers = HEADERS)
    except requests.exceptions.RequestException:
        return None
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

    return month_conf


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
    logging.basicConfig(level=logging.DEBUG,
                        format = '%(levelname)s: %(message)s'
                        )
    urllib3.disable_warnings()

    # GCS Client
    client = storage.Client.from_service_account_json("credentials.json")

    # Target month should be in the URL (in python args)
    print(f"Fetching earthquake records from {month_link}")
    html = parsed_request(month_link)
    month_name = html.select_one('div > table.MsoNormalTable:nth-of-type(2) strong').text

    # Has the target month already been scraped? (Useful in scraping same month multiple times like in fetching the latest data)
    month_conf =  f'conf/{month_name}.json'
    previous_targets = []
    if object_exists(client, month_conf):
        logging.debug("Downloading month's previous conf file %s", month_conf)
        download_object(client, month_conf, month_conf)
        previous_targets.extend(read_json(month_conf))

    # What are the records stored in the current month?
    current_targets = download_links(html)

    # Target the records that were not included in the already scraped items
    targets = [a for a in previous_targets if not a['scraped']]
    new_items = []
    for t in current_targets:        
        if t['url'] not in [a['url'] for a in previous_targets]:
            new_items.append(t)
    targets.extend(new_items)

    # Exit the app if there are no new records to scrape
    if not targets and not new_items:
        logging.info("No new record for %s to scrape", month_name)
        os._exit(1)
    
    print(f"Found {len(targets)} items to be scraped")

    # Save the new records to the month conf
    previous_targets.extend(new_items)
    write_json(previous_targets, month_conf)
    print(f"List of targets saved to ./conf/{month_name}.json")
    upload_to_gcs(client, month_conf, folder = 'conf')
    logging.info("%s file has been uploaded to open-quake1 bucket in %s folder", month_conf, 'conf')

    # Start fetching the month's all data
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

    # Upload updated conf to GCS
    update_conf(month_conf, previous_targets, finished_urls)
    upload_to_gcs(client, month_conf, folder = 'conf')
    
    
    # Transform and save data into parquet
    if data:
        parquet_filename = f"/tmp/{month_name} - p{len(glob(month_name + '*')) + 1:02}"
        into_parquet(data, parquet_filename)
        print(f"Data saved into {parquet_filename}.parquet")

        # Upload parquet to GCS
        upload_to_gcs(client, f"{parquet_filename}.parquet")
        logging.info("%s file has been uploaded to open-quake1 bucket", f"{parquet_filename}.parquet")
        print(f"{parquet_filename}.parquet successfully uploaded to GCS")
        print(f"Data saved into {parquet_filename}.parquet")    

    # Program Completion
    return f"Process Complete. Total {len(finished_urls)}/{len(targets)} targets successfully scraped."