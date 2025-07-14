import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin 

def scrape_books_by_category():
    base_url = 'http://books.toscrape.com/'
    all_scraped_data = []

    print("Starting web scraping process...")

    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f'Failed to fetch base page {base_url}: {e}')
        return

    soup = BeautifulSoup(response.text, 'html.parser') 
    category_links = soup.select('ul.nav-list li a')

    if not category_links:
        print("No category links found on the base page. Check selector.")
        return

    print(f"Found {len(category_links)} potential category links.")

    for link in category_links:
        category_name = link.text.strip()
        if category_name == 'Books' or category_name == 'Category':
            continue

        relative_category_path = link['href']
        current_category_page_url = urljoin(base_url, relative_category_path)

        print(f"\n--- Scraping category: '{category_name}' from {current_category_page_url} ---")
        
        page_num = 1
        while True:
            print(f"  Fetching page {page_num} for '{category_name}' from {current_category_page_url}")

            try:
                category_response = requests.get(current_category_page_url)
                category_response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f'  Failed to fetch category page {current_category_page_url}: {e}')
                break

            category_soup = BeautifulSoup(category_response.text, 'html.parser')

            books = category_soup.select('article.product_pod')

            if not books:
                print(f"  No books found on page {page_num} for '{category_name}'.")
                break

            for book in books:
                try:
                    title_element = book.h3.a
                    title = title_element['title'].strip() if title_element and 'title' in title_element.attrs else 'N/A'

                    price_element = book.select_one('.price_color')
                    price_str = price_element.text.replace('Â£', '').strip() if price_element else '0.00'
                    price = float(price_str)

                    availability_element = book.select_one('.instock.availability')
                    availability_text = availability_element.text.strip() if availability_element else 'N/A'

                    star_rating_element = book.select_one('.star-rating')
                    rating = star_rating_element['class'][1] if star_rating_element and len(star_rating_element['class']) > 1 else 'No rating'

                    all_scraped_data.append({
                        'Category': category_name,
                        'Title': title,
                        'Price': price,
                        'Availability': availability_text,
                        'Star Rating': rating
                    })

                except (AttributeError, ValueError, IndexError) as e:
                    print(f"  Skipping a book in '{category_name}' (Page {page_num}) due to parsing error: {e}. Book HTML snippet: {book}")
                    continue

            next_button = category_soup.select_one('li.next a')

            if next_button:
                current_category_page_url = urljoin(current_category_page_url, next_button['href'])
                page_num += 1
                time.sleep(0.5)
            else:
                print(f"  No more pages found for category '{category_name}'.")
                break

    if all_scraped_data:
        df = pd.DataFrame(all_scraped_data)
        output_filename = 'raw_data.csv'
        try:
            df.to_csv(output_filename, index=False, encoding='utf-8')
            print(f"\nCSV file '{output_filename}' successfully created locally. Total books scraped: {len(df)}")
        except Exception as e:
            print(f"Error saving CSV file locally: {e}")
    else:
        print("\nNo data was scraped. Check selectors or website availability.")

if __name__ == "__main__":
    scrape_books_by_category()
