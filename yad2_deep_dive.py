import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from typing import Dict

from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper to extract details as JSON
def extract_details_json(soup) -> Dict:
    details = {}
    details_container = soup.find('div', class_='boa-attributes-container')
    if details_container:
        for li in details_container.find_all('li', class_='product-spec__item'):
            label = li.find('div', class_='product-spec__label')
            value = li.find('div', class_='product-spec__value')
            if label and value:
                details[label.text.strip()] = value.text.strip()
    return details

# # Helper to extract seller info
# def extract_seller_info(soup) -> Dict:
#     seller = {}
#     seller_container = soup.find('div', class_='seller-details')
#     if seller_container:
#         name_elem = seller_container.find('p', id='seller-fullname')
#         join_year_elem = seller_container.find('span', id='seller-join-year')
#         ads_count_elem = seller_container.find('span', id='seller-ads-count')
#         seller['seller_name'] = name_elem.text.strip() if name_elem else ''
#         seller['seller_join_year'] = join_year_elem.text.strip() if join_year_elem else ''
#         seller['seller_ads_count'] = ads_count_elem.text.strip() if ads_count_elem else ''
#     return seller

# Helper to extract description
def extract_description(soup) -> str:
    desc_container = soup.find('div', class_='product-description')
    if desc_container:
        desc_elem = desc_container.find('span', class_='boa-product-description-details')
        if desc_elem:
            return desc_elem.text.strip()
    return ''

# Main deep dive function
def deep_dive(input_csv, output_csv, limit=None, delay=1.5):
    # Read input data
    df = pd.read_csv(input_csv, dtype=str)
    
    # Try to load existing output file if it exists
    existing_results = []
    try:
        existing_df = pd.read_csv(output_csv, dtype=str)
        existing_results = existing_df.to_dict('records')
        logging.info(f"Loaded {len(existing_results)} existing results from {output_csv}")
    except FileNotFoundError:
        logging.info(f"No existing output file found at {output_csv}")
    
    # Create a set of already processed URLs
    processed_urls = {record.get('product_url') for record in existing_results if record.get('product_url')}
    
    # Filter out already processed listings
    new_listings = df[~df['product_url'].isin(processed_urls)]
    logging.info(f"Found {len(new_listings)} new listings to process")
    
    results = existing_results.copy()  # Start with existing results
    
    for idx, row in tqdm(new_listings.iterrows(), total=len(new_listings)):
        if limit and idx >= limit:
            break
        url = row.get('product_url')
        if not url:
            continue
        logging.info(f"Scraping {url}")
        try:
            resp = requests.get(url, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
            })
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            # Extract fields
            description = extract_description(soup)
            details_json = extract_details_json(soup)
            # Merge all data
            enriched = dict(row)
            enriched['description'] = description
            enriched['details_json'] = json.dumps(details_json, ensure_ascii=False)
            results.append(enriched)
        except Exception as e:
            logging.error(f"Error scraping {url}: {e}")
        time.sleep(delay)
    
    # Save to new CSV
    pd.DataFrame(results).to_csv(output_csv, index=False, encoding='utf-8-sig')
    logging.info(f"Saved deep dive results to {output_csv}")

if __name__ == "__main__":
    # Example usage
    deep_dive(
        input_csv='yad2_collections_ריהוט_min_price_200_max_price_2000_filters_[["Type","ספה בודדת"]].csv',  # Change as needed
        output_csv="yad2_sofas_deep_dive.csv",
        limit=None,  # Set to an integer for testing
        delay=1.5
    )

    