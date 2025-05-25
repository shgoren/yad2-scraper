# Yad2 Scraper

A Python-based scraper for Yad2 listings with image analysis capabilities.

## Setup

1. Clone the repository:
```bash
git clone https://github.com/shgoren/yad2-scraper.git
cd yad2-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file in the root directory with the following content:
```
OPENAI_API_KEY=your_api_key_here
```

## Usage

The scraper includes several scripts for different purposes:

- `yad2_scraper_collections.py`: Main scraper for Yad2 collections
- `yad2_scraper_cars.py`: Scraper specifically for car listings
- `yad2_deep_dive.py`: Deep dive analysis of listings
- `yad2_image_caption_gpt.py`: Image analysis using GPT-4 Vision

## Note

Make sure to never commit your API keys or sensitive information. The `.env` file is already in the `.gitignore` to prevent accidental commits. 