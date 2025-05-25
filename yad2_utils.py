import requests
import logging
import os
import uuid
from urllib.parse import urlparse
import pandas as pd
from typing import List, Dict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Yad2BaseScraper:
    def __init__(self, download_images: bool = False):
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

    def save_to_csv(self, listings: List[Dict], filename: str):
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