import os
import requests
import base64
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
github_token = os.getenv('TOKEN')
repo = 'awakzdev/finance'
branch = 'main'

# Step 1: Fetch today's date in the format day/month/year
today_date = datetime.now().strftime('%Y-%m-%d')

# Symbols to process
symbols = ['QLD', '^NDX']

for symbol in symbols:
    # Step 2: Fetch historical data for the symbol
    data = yf.download(symbol, start='2006-06-21', end=today_date)
    # Was previously - 2019-07-26

    # Convert the index (dates) to the desired format (day/month/year)
    data.index = data.index.strftime('%d/%m/%Y')

    # Step 3: Save the data to a CSV file with the symbol as a prefix
    csv_filename = f'{symbol.lower()}_stock_data.csv'
    data.to_csv(csv_filename)
    file_path_in_repo = csv_filename  # Use the same name for GitHub

    # Step 4: Get the current file's SHA (needed to update a file in the repository)
    url = f'https://api.github.com/repos/{repo}/contents/{file_path_in_repo}'
    headers = {'Authorization': f'token {github_token}'}

    try:
        # Try to get the file's SHA
        response = requests.get(url, headers=headers)
        response_json = response.json()

        if response.status_code == 200:
            # File exists, extract the SHA
            sha = response_json['sha']
            print(f'File {file_path_in_repo} exists, updating it.')
        elif response.status_code == 404:
            # File does not exist, we'll create a new one
            sha = None
            print(f'File {file_path_in_repo} does not exist, creating a new one.')
        else:
            # Other errors
            print(f'Unexpected error: {response_json}')
            continue
    except Exception as e:
        print(f'Error fetching file info for {symbol}: {e}')
        continue

    # Step 5: Read the new CSV file and encode it in base64
    with open(csv_filename, 'rb') as f:
        content = f.read()
    content_base64 = base64.b64encode(content).decode('utf-8')

    # Step 6: Create the payload for the GitHub API request
    commit_message = f'Update {symbol} stock data'
    data = {
        'message': commit_message,
        'content': content_base64,
        'branch': branch
    }

    # Include the SHA if the file exists (for updating)
    if sha:
        data['sha'] = sha

    # Step 7: Push the file to the repository
    response = requests.put(url, headers=headers, json=data)

    # Check if the file was updated/created successfully
    if response.status_code in [200, 201]:
        print(f'File {file_path_in_repo} updated successfully in the repository.')
    else:
        print(f'Failed to update the file {file_path_in_repo} in the repository.')
        print('Response:', response.json())
