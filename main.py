import os
import requests
import base64
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv
import csv
import re

def validate_and_fix_csv(csv_filename):
    """
    Validates the CSV format and fixes it if corrupted.
    
    The correct format should have the header:
    Date,Open,High,Low,Close,Adj Close,Volume
    followed by data lines starting with a date in dd/mm/yyyy format.
    
    If the CSV is corrupted, it removes the incorrect lines and rewrites the CSV.
    
    Args:
        csv_filename (str): The path to the CSV file to validate and fix.
    
    Returns:
        bool: True if the CSV was valid or successfully fixed, False otherwise.
    """
    expected_header = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    date_pattern = re.compile(r'\d{2}/\d{2}/\d{4}')  # Matches dd/mm/yyyy

    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            lines = list(reader)

        # Find the index where the correct header starts
        header_index = -1
        for i, row in enumerate(lines):
            if row == expected_header:
                header_index = i
                break

        if header_index == -1:
            print(f"Header not found in {csv_filename}. The file may be corrupted.")
            return False

        # Extract the relevant rows starting from the header
        valid_rows = lines[header_index:]

        # Verify that there is at least one data row
        if len(valid_rows) < 2:
            print(f"No data found after header in {csv_filename}.")
            return False

        # Validate data rows
        valid_data = [valid_rows[0]]  # Start with the header
        for row in valid_rows[1:]:
            if not row:
                continue  # Skip empty lines
            if date_pattern.match(row[0]):
                # Ensure the row has the correct number of columns
                if len(row) == len(expected_header):
                    valid_data.append(row)
                else:
                    print(f"Skipping row with incorrect number of columns: {row}")
            else:
                print(f"Skipping invalid data row: {row}")

        if len(valid_data) < 2:
            print(f"No valid data found in {csv_filename}.")
            return False

        # Rewrite the CSV with valid data
        with open(csv_filename, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(valid_data)

        print(f"CSV {csv_filename} validated and fixed successfully.")
        return True

    except Exception as e:
        print(f"An error occurred while validating {csv_filename}: {e}")
        return False

def main():
    # Retrieve the GitHub token from environment variables
    load_dotenv()
    
    github_token = os.getenv('TOKEN')
    if not github_token:
        raise ValueError("TOKEN environment variable not set")
    else:
        print(f"TOKEN loaded, length: {len(github_token)} characters")
    
    repo = 'awakzdev/finance-data'
    branch = 'main'
    
    # Step 1: Fetch today's date in the format day/month/year
    today_date = datetime.now().strftime('%Y-%m-%d')
    
    # Symbols to process
    symbols = ['QLD', '^NDX']
    
    for symbol in symbols:
        try:
            # Step 2: Fetch historical data for the symbol
            print(f"Fetching data for symbol: {symbol}")
            data = yf.download(symbol, start='2006-06-21', end=today_date)
            if data.empty:
                print(f"No data fetched for symbol: {symbol}")
                continue

            # Convert the index (dates) to the desired format (day/month/year)
            data.index = data.index.strftime('%d/%m/%Y')

            # Step 3: Save the data to a CSV file with the symbol as a prefix
            sanitized_symbol = symbol.replace('^', '')
            csv_filename = f'{sanitized_symbol.lower()}_stock_data.csv'
            
            # Ensure the DataFrame is saved correctly
            data.to_csv(csv_filename, index=True, index_label='Date')
            print(f"CSV {csv_filename} saved successfully.")

            # Step 4: Validate and fix the CSV if necessary
            if not validate_and_fix_csv(csv_filename):
                print(f"Skipping upload for {csv_filename} due to validation failure.")
                continue  # Skip uploading this file

            # Step 5: Get the current file's SHA (needed to update a file in the repository)
            url = f'https://api.github.com/repos/{repo}/contents/{csv_filename}'
            headers = {'Authorization': f'token {github_token}'}

            response = requests.get(url, headers=headers)
            response_json = response.json()

            if response.status_code == 200:
                # File exists, extract the SHA
                sha = response_json['sha']
                print(f'File {csv_filename} exists, updating it.')
            elif response.status_code == 404:
                # File does not exist, we'll create a new one
                sha = None
                print(f'File {csv_filename} does not exist, creating a new one.')
            else:
                # Other errors
                print(f'Unexpected error while accessing {csv_filename}: {response_json}')
                continue

            # Step 6: Read the new CSV file and encode it in base64
            with open(csv_filename, 'rb') as f:
                content = f.read()
            content_base64 = base64.b64encode(content).decode('utf-8')

            # Step 7: Create the payload for the GitHub API request
            commit_message = f'Update {sanitized_symbol} stock data'
            payload = {
                'message': commit_message,
                'content': content_base64,
                'branch': branch
            }

            # Include the SHA if the file exists (for updating)
            if sha:
                payload['sha'] = sha

            # Step 8: Push the file to the repository
            response = requests.put(url, headers=headers, json=payload)

            # Check if the file was updated/created successfully
            if response.status_code in [200, 201]:
                print(f'File {csv_filename} updated successfully in the repository.')
            else:
                print(f'Failed to update the file {csv_filename} in the repository.')
                print('Response:', response.json())

        except Exception as e:
            print(f'An error occurred while processing symbol {symbol}: {e}')

if __name__ == "__main__":
    main()
