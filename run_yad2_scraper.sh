#!/bin/bash

# Set the working directory to where the scripts are located
cd "$(dirname "$0")"

# Create logs directory if it doesn't exist
mkdir -p logs

# Get current date for log file names
DATE=$(date +"%Y-%m-%d")

# Run the collection scraper
echo "Starting collection scraper at $(date)" >> "logs/collection_scraper_${DATE}.log"
python3 yad2_scraper_collections.py >> "logs/collection_scraper_${DATE}.log" 2>&1
echo "Finished collection scraper at $(date)" >> "logs/collection_scraper_${DATE}.log"

# Wait a bit to ensure files are written
sleep 5

# Run the deep dive
echo "Starting deep dive at $(date)" >> "logs/deep_dive_${DATE}.log"
python3 yad2_deep_dive.py >> "logs/deep_dive_${DATE}.log" 2>&1
echo "Finished deep dive at $(date)" >> "logs/deep_dive_${DATE}.log" 