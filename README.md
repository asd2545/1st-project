# Book Data Scraper and Cleaner
This project is a Python-based solution designed to scrape book data from http://books.toscrape.com/, clean the collected data, and store it either locally or on Google Cloud Storage (GCS). It's developed to practice and sharpen Python skills, particularly in web scraping, data manipulation with Pandas, and cloud function deployment.

Features
Category-wise Scraping: Scrapes books by iterating through all available categories on the target website.
Pagination Handling: Automatically navigates through multiple pages within each category to ensure comprehensive data collection.
Robust Data Cleaning: Implements several cleaning steps:
  Removal of duplicate rows.
  Handling of missing 'Price' values by dropping rows.
  Filtering out books with non-positive prices.
  Stripping whitespace from book titles.
Local Data Storage: Saves raw and cleaned data as CSV files locally.
Google Cloud Platform Integration:
  Scrapes data directly to a specified GCS bucket (raw_data.csv).
  Cleans data from a GCS bucket and saves the cleaned data back to GCS (cleaned_data.csv).
  Designed for deployment as Google Cloud Functions (HTTP triggered).

Project Structure
scrape_data.py: Contains the Python script for scraping book data locally. It fetches data from books.toscrape.com and saves it to raw_data.csv.
clean_data.py: Contains the Python script for cleaning locally stored raw_data.csv and saving the cleaned data to cleaned_data.csv.
GCP scrape_data.py: A modified version of scrape_data.py designed to run as a Google Cloud Function. It scrapes data and uploads raw_data.csv directly to a specified GCS bucket.
GCP clean_data.py: A modified version of clean_data.py designed to run as a Google Cloud Function. It reads raw_data.csv from GCS, cleans it, and uploads cleaned_data.csv back to GCS.
README.md: This file, providing an overview and instructions for the project.
