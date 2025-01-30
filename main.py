import os
import requests
import base64
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv

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
        
        # Step 4: Get the current file's SHA (needed to update a file in the repository)
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

        # Step 5: Read the new CSV file and encode it in base64
        with open(csv_filename, 'rb') as f:
            content = f.read()
        content_base64 = base64.b64encode(content).decode('utf-8')

        # Step 6: Create the payload for the GitHub API request
        commit_message = f'Update {sanitized_symbol} stock data'
        payload = {
            'message': commit_message,
            'content': content_base64,
            'branch': branch
        }

        # Include the SHA if the file exists (for updating)
        if sha:
            payload['sha'] = sha

        # Step 7: Push the file to the repository
        response = requests.put(url, headers=headers, json=payload)

        # Check if the file was updated/created successfully
        if response.status_code in [200, 201]:
            print(f'File {csv_filename} updated successfully in the repository.')
        else:
            print(f'Failed to update the file {csv_filename} in the repository.')
            print('Response:', response.json())

    except Exception as e:
        print(f'An error occurred while processing symbol {symbol}: {e}')
