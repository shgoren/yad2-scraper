import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
from typing import List, Dict
import logging
import uuid
import os
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Yad2Scraper:
    def __init__(self, download_images: bool = False):
        self.base_url = "https://www.yad2.co.il/api/feed"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.yad2.co.il/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        }
        # Create images directory if it doesn't exist
        self.images_dir = "images"
        self.download_images = download_images
        os.makedirs(self.images_dir, exist_ok=True)
        # Create a session object
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def debug_request(self, url, params=None):
        """
        Print detailed information about the request and response for debugging
        """
        print("\n=== Request Details ===")
        print(f"URL: {url}")
        print("\nHeaders:")
        for key, value in self.session.headers.items():
            print(f"{key}: {value}")
        if params:
            print("\nParameters:")
            for key, value in params.items():
                print(f"{key}: {value}")
        
        response = self.session.get(url, params=params)
        print("\n=== Response Details ===")
        print(f"Status Code: {response.status_code}")
        print("\nResponse Headers:")
        for key, value in response.headers.items():
            print(f"{key}: {value}")
        print("\nResponse Content:")
        print(response.text[:1000])  # Print first 1000 chars
        return response

    def download_image(self, image_url: str) -> str:
        """
        Download an image and save it with a UUID filename
        Returns the local path to the saved image
        """
        try:
            # Generate a unique filename with the original extension
            file_extension = os.path.splitext(urlparse(image_url).path)[1]
            if not file_extension:
                file_extension = '.jpg'  # Default to jpg if no extension found
            filename = f"{uuid.uuid4()}{file_extension}"
            filepath = os.path.join(self.images_dir, filename)

            # Download and save the image
            response = requests.get(image_url, headers=self.headers)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            logging.info(f"Downloaded image: {filename}")
            return filepath

        except Exception as e:
            logging.error(f"Error downloading image {image_url}: {e}")
            return ""

    def get_listing_details(self, url: str) -> Dict:
        """
        Fetch and parse details from an individual listing page
        """
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            details = {}
            
            # Get description
            desc_elem = soup.find('p', class_='description_description__xxZXs')
            details['description'] = desc_elem.text.strip() if desc_elem else ''
            
            # Get additional details
            details_section = soup.find('section')
            if details_section:
                labels = details_section.find_all('dd', class_='item-detail_label__FnhAu')
                values = details_section.find_all('dt', class_='item-detail_value__QHPml')
                
                for label, value in zip(labels, values):
                    key = label.text.strip()
                    val = value.text.strip()
                    details[key] = val
            
            return details
            
        except Exception as e:
            logging.error(f"Error fetching listing details from {url}: {e}")
            return {}

    def search_listings(self, category: str = "cars", manufacturer: str = None, 
                       model: str = None, year: str = None, 
                       min_price: int = None, max_price: int = None, 
                       page: int = 1) -> List[Dict]:
        """
        Search for listings on Yad2 with given parameters
        """
        # Construct the URL with query parameters
        url = f"https://www.yad2.co.il/vehicles/cars"
        params = {
            "manufacturer": manufacturer,
            "model": model,
            "year": year,
            "priceOnly": "1",
            "page": page
        }
        
        # Format price parameter according to Yad2's format
        if min_price is not None or max_price is not None:
            min_price_str = str(min_price) if min_price is not None else "-1"
            max_price_str = str(max_price) if max_price is not None else "-1"
            params["price"] = f"{min_price_str}-{max_price_str}"

        try:
            # First visit the main page to get cookies
            self.session.get("https://www.yad2.co.il/")
            time.sleep(0.5)  # Wait a bit before making the request
            
            # Make the request
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # Parse the HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all listing items
            listings = []
            for item in soup.find_all('div', class_='feed-item-base_feedItemBox__5WVY1'):
                try:
                    # Extract the link
                    link_elem = item.find('a', class_='feed-item-base_itemLink__wBfEL')
                    link = link_elem['href'] if link_elem else ''
                    if link:
                        # Clean the URL by removing query parameters
                        parsed_url = urlparse(link)
                        clean_link = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                        if not clean_link.startswith('http'):
                            clean_link = f"https://www.yad2.co.il{clean_link}"
                        link = clean_link
                    
                    # Extract the image URL
                    img_elem = item.find('img', class_='single-image_image__Iv6T9')
                    image_url = ''
                    if img_elem and 'srcset' in img_elem.attrs:
                        # Get the highest resolution image from srcset
                        srcset = img_elem['srcset']
                        image_url = srcset.split(',')[-1].strip().split(' ')[0]
                    
                    # Extract title and model details
                    title_elem = item.find('span', class_='feed-item-info_heading__k5pVC')
                    model_elem = item.find('span', class_='feed-item-info_marketingText__eNE4R')
                    year_elem = item.find('span', class_='feed-item-info_yearAndHandBox___JLbc')
                    
                    # Extract agency and price
                    agency_elem = item.find('span', class_='commercial-item-left-side_agencyName__psfbp')
                    price_elem = item.find('span', class_='price_price__xQt90')
                    monthly_payment_elem = item.find('span', class_='monthly-payment_monthlyPaymentBox__9nxfH')
                    
                    # Download the image
                    if self.download_images:
                        image_path = self.download_image(image_url) if image_url else ''
                        time.sleep(0.3)  # Small delay between image downloads
                    else:
                        image_path = image_url
                    
                    # Get additional details from the listing page
                    # listing_details = self.get_listing_details(link) if link else {}
                    
                    listing = {
                        'title': title_elem.text.strip() if title_elem else '',
                        'model_details': model_elem.text.strip() if model_elem else '',
                        'year': year_elem.text.strip() if year_elem else '',
                        'agency': agency_elem.text.strip() if agency_elem else '',
                        'price': price_elem.text.strip() if price_elem else '',
                        'monthly_payment': monthly_payment_elem.text.strip() if monthly_payment_elem else '',
                        'link': link,
                        'image_path': image_path,
                        # 'description': listing_details.get('description', ''),
                        # 'kilometers': listing_details.get('קילומטראז׳', ''),
                        # 'color': listing_details.get('צבע', ''),
                        # 'current_ownership': listing_details.get('בעלות נוכחית', ''),
                        # 'test_until': listing_details.get('טסט עד', ''),
                        # 'previous_ownership': listing_details.get('בעלות קודמת', ''),
                        # 'transmission': listing_details.get('תיבת הילוכים', ''),
                        # 'road_date': listing_details.get('תאריך עליה לכביש', ''),
                        # 'engine_type': listing_details.get('סוג מנוע', ''),
                        # 'body_type': listing_details.get('מרכב', ''),
                        # 'seats': listing_details.get('מושבים', ''),
                        # 'horsepower': listing_details.get('כוח סוס', ''),
                        # 'engine_volume': listing_details.get('נפח מנוע', ''),
                        # 'fuel_consumption': listing_details.get('צריכת דלק משולבת', '')
                    }
                    listings.append(listing)
                    time.sleep(0.5)  # Small delay between listing requests
                    
                except Exception as e:
                    logging.error(f"Error parsing listing: {e}")
                    continue

            return listings

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            return []

    def save_to_csv(self, listings: List[Dict], filename: str = "yad2_listings.csv"):
        """
        Save listings to a CSV file using a temporary file for safety
        """
        if not listings:
            logging.warning("No listings to save")
            return

        # Create a temporary filename
        temp_filename = f"{filename}.temp"
        
        try:
            # Save to temporary file first
            df = pd.DataFrame(listings)
            df.to_csv(temp_filename, index=False, encoding='utf-8-sig')
            
            # If successful, replace the original file
            if os.path.exists(filename):
                os.remove(filename)
            os.rename(temp_filename, filename)
            
            logging.info(f"Saved {len(listings)} listings to {filename}")
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")
            # Clean up temporary file if it exists
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            raise

def main():
    scraper = Yad2Scraper()
    
    # Example search parameters for cars
    search_params = {
        "category": "cars",
        "manufacturer": "35",  # Example manufacturer ID
        "model": None,      # Example model ID
        "year": None,   # Year range
        "min_price": None,        # Minimum price
        "max_price": None     # Maximum price
    }

    # Get listings from all available pages
    all_listings = []
    page = 1
    output_file = "yad2_listings.csv"
    
    while True:
        logging.info(f"Fetching page {page}")
        listings = scraper.search_listings(
            category=search_params["category"],
            manufacturer=search_params["manufacturer"],
            model=search_params["model"],
            year=search_params["year"],
            min_price=search_params["min_price"],
            max_price=search_params["max_price"],
            page=page
        )
        
        # If no listings found, we've reached the end
        if not listings:
            logging.info(f"No more listings found on page {page}. Stopping pagination.")
            break
            
        all_listings.extend(listings)
        
        # Save checkpoint after each page
        scraper.save_to_csv(all_listings, output_file)
        logging.info(f"Saved checkpoint to {output_file}")
        
        page += 1
        time.sleep(1)  # Be nice to the server

    logging.info(f"Total listings found: {len(all_listings)}")
    logging.info(f"Final results saved to {output_file}")

if __name__ == "__main__":
    main()


