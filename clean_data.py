import pandas as pd
import os

SOURCE_FILE_NAME = 'raw_data.csv'
CLEANED_FILE_NAME = 'cleaned_data.csv'

def clean_and_store_data_local():
    print(f"Starting data cleaning process for {SOURCE_FILE_NAME} (local file)...")

    try:
        if not os.path.exists(SOURCE_FILE_NAME):
            raise FileNotFoundError(f"Source file not found: {SOURCE_FILE_NAME}")

        df = pd.read_csv(SOURCE_FILE_NAME)
        print(f"Successfully read {len(df)} rows from {SOURCE_FILE_NAME}.")

        print("Applying data cleaning steps...")

        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        print(f"Removed {initial_rows - len(df)} duplicate rows. New row count: {len(df)}")

        print("\nMissing values before cleaning:")
        print(df.isnull().sum())

        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        print("\nAfter ensuring 'Price' is numeric and coercing errors:")
        print(df.isnull().sum())

        initial_price_nans = df['Price'].isnull().sum()
        if initial_price_nans > 0:
            df.dropna(subset=['Price'], inplace=True)
            print(f"Removed {initial_price_nans} rows due to unparseable 'Price' values.")

        initial_rows = len(df)
        df = df[df['Price'] > 0]
        print(f"Removed {initial_rows - len(df)} rows with Price <= 0. New row count: {len(df)}")

        df['Title'] = df['Title'].str.strip()
        print("Applied text cleaning (strip whitespace) to 'Title' column.")

        print("\nData cleaning complete. First 5 rows of cleaned data:")
        print(df.head())
        print(f"\nFinal cleaned data rows: {len(df)}")

        df.to_csv(CLEANED_FILE_NAME, index=False, encoding='utf-8')
        print(f"Cleaned data successfully saved to {CLEANED_FILE_NAME}")
        return "Data cleaning successful!", 200

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return f"Data cleaning failed: {e}", 404
    except Exception as e:
        print(f"An error occurred during data cleaning or storage: {e}")
        return f"Data cleaning failed: {e}", 500

if __name__ == "__main__":
    print("Running data cleaning directly for local testing...")
    message, code = clean_and_store_data_local()
    print(f"Local execution result: {message} (Status Code: {code})")
