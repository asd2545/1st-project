import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import functions_framework
import logging
import json
from google.cloud import storage
import io

logging.basicConfig(level=logging.INFO)

@functions_framework.http
def scrape_books_http(request):
    logging.info("Received HTTP request to trigger book scraping.")
    
    gcs_bucket_name = 'run-sources-st-project-464315-asia-southeast1' 
    gcs_file_name = 'raw_data.csv'

    try:
        scraped_data = scrape_books_by_category()
        if scraped_data:
            df = pd.DataFrame(scraped_data)
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            upload_to_gcs(csv_buffer.getvalue(), gcs_bucket_name, gcs_file_name)
            logging.info(f"Successfully scraped and uploaded data to gs://{gcs_bucket_name}/{gcs_file_name}")
            
            return json.dumps(scraped_data), 200, {'Content-Type': 'application/json'}
        else:
            logging.warning("No data was scraped.")
            return "No data was scraped. Check selectors or website availability.", 204
    except Exception as e:
        logging.error(f"An error occurred during scraping or GCS upload: {e}", exc_info=True)
        return f"An internal server error occurred: {e}", 500

def upload_to_gcs(data_string, bucket_name, destination_blob_name):
    logging.info(f"Attempting to upload data to gs://{bucket_name}/{destination_blob_name}")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_string(data_string, content_type='text/csv')
        logging.info(f"File {destination_blob_name} uploaded to {bucket_name}.")
    except Exception as e:
        logging.error(f"Failed to upload to GCS: {e}", exc_info=True)
        raise

def scrape_books_by_category():
    base_url = 'http://books.toscrape.com/'
    all_scraped_data = []
    logging.info("Starting web scraping process...")

    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to fetch base page {base_url}: {e}')
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    category_links = soup.select('div.side_categories ul.nav-list li a')

    if not category_links:
        logging.warning("No category links found on the base page. Check selector.")
        return []

    logging.info(f"Found {len(category_links)} potential category links.")

    for link in category_links:
        category_name = link.text.strip()
        
        if category_name == 'Books' or category_name == 'Category':
            continue

        relative_category_path = link['href']        
        current_category_page_url = urljoin(base_url, relative_category_path)
        logging.info(f"\n--- Scraping category: '{category_name}' from {current_category_page_url} ---")
        
        page_num = 1
        while True:
            logging.info(f"   Fetching page {page_num} for '{category_name}' from {current_category_page_url}")

            try:
                category_response = requests.get(current_category_page_url)
                category_response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f'   Failed to fetch category page {current_category_page_url}: {e}')
                break

            category_soup = BeautifulSoup(category_response.text, 'html.parser')
            books = category_soup.select('article.product_pod')

            if not books:
                logging.info(f"   No books found on page {page_num} for '{category_name}'.")
                break

            for book in books:
                try:
                    title_element = book.h3.a
                    title = title_element['title'].strip() if title_element and 'title' in title_element.attrs else 'N/A'

                    price_element = book.select_one('.price_color')
                    price_str = price_element.text.replace('Â£', '').replace('£', '').strip() if price_element else '0.00'
                    price = float(price_str)
                    
                    availability_element = book.select_one('.instock.availability')
                    availability_text = availability_element.text.strip() if availability_element else 'N/A'
                    
                    star_rating_element = book.select_one('.star-rating')
                    rating_map = {'One': '1 Star', 'Two': '2 Stars', 'Three': '3 Stars', 'Four': '4 Stars', 'Five': '5 Stars'}
                    rating_class = star_rating_element['class'][1] if star_rating_element and len(star_rating_element['class']) > 1 else 'No rating'
                    rating = rating_map.get(rating_class, rating_class)

                    all_scraped_data.append({
                        'Category': category_name,
                        'Title': title,
                        'Price': price,
                        'Availability': availability_text,
                        'Star Rating': rating
                    })
                except (AttributeError, ValueError, IndexError) as e:
                    logging.error(f"   Skipping a book in '{category_name}' (Page {page_num}) due to parsing error: {e}. Book HTML snippet: {book}")
                    continue

            next_button = category_soup.select_one('li.next a')

            if next_button:                
                current_category_page_url = urljoin(current_category_page_url, next_button['href'])
                page_num += 1
                time.sleep(0.5) 
            else:
                logging.info(f"   No more pages found for category '{category_name}'.")
                break
    
    logging.info(f"Scraping completed. Total books scraped: {len(all_scraped_data)}")
    return all_scraped_data
