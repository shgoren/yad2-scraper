import time
import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Set
from yad2_utils import Yad2BaseScraper
from yad2_categories import COLLECTIONS
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

class Yad2CollectionsScraper(Yad2BaseScraper):
    def __init__(self, download_images: bool = False, headless: bool = True):
        super().__init__(download_images)
        self.base_url = "https://www.yad2.co.il/market/collections"
        self.headless = headless
        
        # Set up Chrome options
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument(f'user-agent={self.headers["User-Agent"]}')
        
        # Initialize the Chrome driver
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        
        # For tracking listings
        self.existing_ids: Set[str] = set()  # Set of product IDs seen in current scrape
        self.df_existing = None
        self.output_file = None

    def __del__(self):
        """
        Clean up the driver when the object is destroyed
        """
        if hasattr(self, 'driver'):
            self.driver.quit()

    def load_existing_product_ids(self, output_file: str):
        """
        Load existing product IDs from the CSV file if it exists
        """
        self.output_file = output_file
        if os.path.exists(output_file):
            try:
                df = pd.read_csv(output_file, dtype=str)
                self.df_existing = df
                if 'product_id' in df.columns:
                    # Get all active listings (those without a closing_date)
                    self.existing_ids = set(df['product_id'].dropna().astype(str))
                    logging.info(f"Loaded {len(self.existing_ids)} listings from {output_file}")
            except Exception as e:
                logging.error(f"Error loading existing product IDs: {e}")
                self.existing_ids = set()
                self.df_existing = None
        else:
            self.existing_ids = set()
            self.df_existing = None

    def get_collection_name(self, url: str) -> str:
        """
        Extract collection name from URL
        """
        try:
            # Remove query parameters and get the last part of the path
            path = url.split('?')[0].split('/')[-1]
            return path
        except Exception as e:
            logging.error(f"Error extracting collection name from {url}: {e}")
            return "unknown_collection"

    # def update_listing_status(self, current_listings: Set[str]):
    #     """
    #     Update the status of listings in the CSV file
    #     """
    #     if self.df_existing is not None:
    #         # Find listings that were active but not seen in current scrape
    #         closed_listings = self.active_listings - current_listings
            
    #         if closed_listings:
    #             # Update closing date for closed listings
    #             mask = (self.df_existing['product_id'].astype(str).isin(closed_listings)) & \
    #                    (self.df_existing['closing_date'].isna())
    #             self.df_existing.loc[mask, 'closing_date'] = datetime.now().strftime('%Y-%m-%d')
    #             self.df_existing.to_csv(self.output_file, index=False, encoding='utf-8-sig')
    #             logging.info(f"Marked {len(closed_listings)} listings as closed")
            
    #         # Update active listings set
    #         self.active_listings = self.active_listings - closed_listings

    def parse_product_card(self, card) -> Dict:
        """
        Parse a single product card and extract relevant information
        """
        try:
            # Skip if it's a new business listing
            if card.find('div', class_='item-image_newBusinessTag__zI6xW'):
                return {}

            # Get product URL and ID from the link
            link = card.find('a', class_='item-grid_imageContainer__U2drL')
            product_url = f"https://www.yad2.co.il{link.get('href', '')}" if link else ''
            product_id = product_url.split('/')[-1] if product_url else ''

            # Get image URL
            img_elem = card.find('img', class_='shopify-image_image__KPxpT')
            image_url = img_elem.get('src', '') if img_elem else ''

            # Get price information
            price_elem = card.find('p', class_='item-price_price__HMXoj')
            current_price = price_elem.text.strip().replace('₪', '') if price_elem else ''

            # Get location
            location_elem = card.find('p', class_='item-location_location__E96ST')
            location = location_elem.text.strip() if location_elem else ''

            # Get title
            title_elem = card.find('h2', class_='item-title_title__2tG20')
            title = title_elem.text.strip() if title_elem else ''

            # Get tags/condition
            tags_container = card.find('div', class_='item-tags_tags__GdgQO')
            tags = [tag.text.strip() for tag in tags_container.find_all('p', class_='tag_tag__Zaq8_')] if tags_container else []

            # Download image if enabled
            if self.download_images and image_url:
                image_path = self.download_image(image_url)
                time.sleep(0.3)  # Small delay between image downloads

            current_date = datetime.now().strftime('%Y-%m-%d')
            
            return {
                'product_id': product_id,
                'title': title,
                'current_price': current_price,
                'location': location,
                'image_url': image_url,
                'product_url': product_url,
                'tags': ', '.join(tags),
                'first_seen_date': current_date,
                'last_seen_date': current_date,
                'closing_date': None
            }

        except Exception as e:
            logging.error(f"Error parsing product card: {e}")
            return {}

    def wait_for_products(self, timeout: int = 10) -> bool:
        """
        Wait for products to load and return True if products are found
        """
        try:
            # Wait for the feed container to appear
            self.wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "feed_feedContainer__Abipd"))
            )
            return True
        except TimeoutException:
            logging.info("No products found on this page")
            return False
        except Exception as e:
            logging.error(f"Error waiting for products: {e}")
            return False

    def scroll_to_load_more(self, max_scrolls: int = 3) -> bool:
        """
        Scroll to the bottom of the page to load more content via infinite scroll
        Returns True if new content was loaded, False otherwise
        """
        try:
            # Get initial number of products
            initial_products = len(self.driver.find_elements(By.CSS_SELECTOR, "article.item-grid_grid__P3tKb"))
            
            for scroll_attempt in range(max_scrolls):
                # Scroll to the bottom of the page
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Wait for new content to potentially load
                time.sleep(2)
                
                # Check if new products were loaded
                current_products = len(self.driver.find_elements(By.CSS_SELECTOR, "article.item-grid_grid__P3tKb"))
                if current_products == initial_products:
                    time.sleep(3
                    )
                    current_products = len(self.driver.find_elements(By.CSS_SELECTOR, "article.item-grid_grid__P3tKb"))
                if current_products > initial_products:
                    logging.info(f"Loaded {current_products - initial_products} more products via scrolling")
                    return True
                    
            return False
        except Exception as e:
            logging.error(f"Error during scrolling: {e}")
            return False

    def detect_captcha(self) -> bool:
        """
        Detect if a CAPTCHA is present on the page
        """
        try:
            # Look for the specific CAPTCHA wrapper
            if self.driver.find_elements(By.CSS_SELECTOR, "div.captcha-wrapper"):
                return True
                    
            return False
        except Exception as e:
            logging.error(f"Error detecting CAPTCHA: {e}")
            return False

    def restart_driver_without_headless(self):
        """
        Restart the driver in non-headless mode for CAPTCHA solving
        """
        try:
            logging.info("CAPTCHA detected! Restarting browser in visible mode...")
            current_url = self.driver.current_url
            
            # Close the current driver
            self.driver.quit()
            
            # Restart without headless
            self.chrome_options = Options()
            self.chrome_options.add_argument('--no-sandbox')
            self.chrome_options.add_argument('--disable-dev-shm-usage')
            self.chrome_options.add_argument(f'user-agent={self.headers["User-Agent"]}')
            
            self.driver = webdriver.Chrome(options=self.chrome_options)
            self.wait = WebDriverWait(self.driver, 10)
            
            # Navigate back to the current URL
            self.driver.get(current_url)
            self.headless = False
            
            logging.info("Browser restarted in visible mode. Please solve the CAPTCHA manually.")
            logging.info("Waiting for CAPTCHA to be solved...")
            
            # Wait for CAPTCHA to be solved (captcha-wrapper to disappear)
            self.wait_for_captcha_solved()
            
        except Exception as e:
            logging.error(f"Error restarting driver: {e}")

    def wait_for_captcha_solved(self, timeout: int = 300):
        """
        Wait for the CAPTCHA to be solved by monitoring when captcha-wrapper disappears
        """
        try:
            # Create a longer wait for CAPTCHA solving (5 minutes max)
            captcha_wait = WebDriverWait(self.driver, timeout)
            
            # Wait for the captcha-wrapper to disappear
            captcha_wait.until_not(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.captcha-wrapper"))
            )
            
            logging.info("CAPTCHA solved! Continuing with scraping...")
            
        except TimeoutException:
            logging.warning(f"CAPTCHA not solved within {timeout} seconds. You may need to solve it manually.")
            logging.info("Press Enter to continue anyway...")
            input("Waiting for manual confirmation...")
        except Exception as e:
            logging.error(f"Error waiting for CAPTCHA to be solved: {e}")
            logging.info("Press Enter to continue...")
            input("Waiting for manual confirmation...")

    def search_collection(self, collection_url: str, page: int = 1, filters: Dict = None) -> List[Dict]:
        """
        Search for products in a specific collection with optional filters
        """
        try:
            # Construct the URL with page parameter and filters
            params = {}
            if page > 1:
                params['pageNumber'] = page
            
            if filters:
                if 'min_price' in filters:
                    params['minPrice'] = filters['min_price']
                if 'max_price' in filters:
                    params['maxPrice'] = filters['max_price']
                if 'productTypes' in filters:
                    params['productTypes'] = filters['productTypes']
            
            # Always sort by newest
            params['sortOption'] = 'newest'
            
            url = f"{collection_url}?{urlencode(params)}"
            
            # Load the page with Selenium
            self.driver.get(url)
            
            # Check for CAPTCHA
            if self.detect_captcha() and self.headless:
                self.restart_driver_without_headless()
            
            # Wait for products to load
            if not self.wait_for_products():
                return [], True

            # If this is page 1, try scrolling to load more content
            if self.scroll_to_load_more(max_scrolls=2):
                while self.scroll_to_load_more(max_scrolls=2):
                    pass

            # Get the page source after JavaScript has loaded the content
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find all product cards
            product_cards = soup.find_all('article', class_='item-grid_grid__P3tKb')
            
            # Parse each product card with progress bar
            listings = []
            current_listings = set()
            if len(product_cards) == 0:
                logging.info(f"No products found on page {page}")
                return [], True
            
            for card in tqdm(product_cards, desc=f"Processing page {page}", leave=False):
                listing = self.parse_product_card(card)
                if listing:
                    product_id = listing.get('product_id')
                    # Update last_seen_date for existing listings
                    if self.df_existing is not None:
                        mask = self.df_existing['product_id'].astype(str) == str(product_id)
                        if mask.any():
                            self.df_existing.loc[mask, 'last_seen_date'] = listing['last_seen_date']
                            self.df_existing.loc[mask, 'current_price'] = listing['current_price']
                    else:
                        listings.append(listing)
            
            return listings, False

        except Exception as e:
            logging.error(f"Error searching collection {collection_url}: {e}")
            return [], True

    def scrape_category(self, category_key: str, filters: Dict = None) -> None:
        """
        Scrape all products from a specific category with optional filters
        """
        if category_key not in COLLECTIONS:
            logging.error(f"Category {category_key} not found in COLLECTIONS")
            return

        category = COLLECTIONS[category_key]
        collection_url = category['url']
        collection_name = self.get_collection_name(collection_url)

        applied_filters = {k:v for k,v in filters.items() if v is not None}
        
        # Add filters to filename if present
        if filters:
            filter_str = '_'.join(f"{k}_{v}" for k, v in applied_filters.items())
            output_file = f"yad2_collections_{collection_name}_{filter_str}.csv"
        else:
            output_file = f"yad2_collections_{collection_name}.csv"
        
        logging.info(f"Starting to scrape category: {category['name']}")
        if applied_filters:
            logging.info(f"With filters: {applied_filters}")
        
        # Load existing product IDs
        self.load_existing_product_ids(output_file)
        
        # Get listings from all available pages
        all_listings = []
        page = 1
        stop_scraping = False
        
        # Create progress bar for pages
        # with tqdm(desc=f"Scraping {category['name']}", unit="page") as pbar:
            # while not stop_scraping :  # Limit to 5 pages since most content is on page 1 with scrolling
                # logging.info(f"Fetching page {page}")
        listings, stop_scraping = self.search_collection(collection_url, page, applied_filters)
                # if stop_scraping or len(listings) == 0:
                #     break
                    
        all_listings.extend(listings)
                
        # Save checkpoint after each page
        self.save_to_csv(all_listings, output_file)
        logging.info(f"Saved checkpoint to {output_file}")
                
                # page += 1
                # pbar.update(1)
                # time.sleep(1)  # Be nice to the server

        logging.info(f"Total listings found for {category['name']}: {len(all_listings)}")
        logging.info(f"Final results saved to {output_file}")

    def save_to_csv(self, listings: List[Dict], output_file: str):
        """
        Save listings to CSV file, preserving existing data
        """
        try:
            df_new = pd.DataFrame(listings)
            
            if os.path.exists(output_file):
                # Read existing data
                
                # Combine existing and new data
                df_combined = pd.concat([self.df_existing, df_new], ignore_index=True)
                
                # Remove duplicates based on product_id and last_seen_date
                df_combined = df_combined.drop_duplicates(subset=['product_id', 'last_seen_date'], keep='last')
                
                # Sort by last_seen_date and product_id
                df_combined = df_combined.sort_values(['last_seen_date', 'product_id'], ascending=[False, True])
                
                df_combined.to_csv(output_file, index=False, encoding='utf-8-sig')
                logging.info(f"Updated {output_file} with {len(listings)} new listings")
            else:
                df_new.to_csv(output_file, index=False, encoding='utf-8-sig')
                logging.info(f"Created new file {output_file} with {len(listings)} listings")
                
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")

def process_queries(scraper, queries):
    """
    Process multiple scraping queries with their respective filters
    """
    for query in queries:
        category_key = query.get('category_key')
        filters = query.get('filters', {})
        
        logging.info(f"Processing category: {category_key}")
        if filters:
            logging.info(f"With filters: {filters}")
            
        scraper.scrape_category(category_key, filters)
        time.sleep(2)  # Be nice between categories

def main():
    # Start in headless mode for speed, but will switch to visible mode if CAPTCHA is detected
    scraper = Yad2CollectionsScraper(download_images=False, headless=True)
    
    # Example queries with different filters
    queries = [
        {
            'category_key': 'furniture',
            'filters': {
                'min_price': '200',
                'max_price': '2000',
                'productTypes': 460
            }
        },
        {
            'category_key': 'electronics_earphones',
            'filters': {
                'min_price': '200',
                'max_price': '2000'
            }
        },
        {
            'category_key': 'bikes_and_scooters',
            'filters': {
                'min_price': '200',
                'max_price': '2000'
            }
        },
        {
            'category_key': 'cell_phones',
            'filters': {
                'min_price': '200',
                'max_price': '2000'
            }
        }
    ]
    
    process_queries(scraper, queries)

if __name__ == "__main__":
    main()