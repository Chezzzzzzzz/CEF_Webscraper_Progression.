import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def fetch_data_from_url(url, delay=10):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    while True:  # Keep trying indefinitely
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                fund_data = {}
                elements = soup.find_all('td')
                for i in range(len(elements) - 1):  # Avoid out-of-range error
                    key = elements[i].text.strip()
                    "UNII / Share" in key or "Earnings / Share" in key or key in ["Average Discount (3 Yr)", "Market Yield",
                       "Current Distribution",
                        "Div Growth (3yr)", "Earn Coverage"]

                if fund_data:  # Successfully fetched data
                    return fund_data

            else:
                print(f"Failed to fetch data, status code: {response.status_code}. Retrying in {delay} seconds...")
                time.sleep(delay)

        except Exception as e:
            print(f"Error fetching data from {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)

# List of tickers
tickers = ["KTF", "MAV", "MHI"
]


# Prepare a list to collect data for all tickers
all_data = []

# Loop through each ticker
for ticker in tickers:
    second_url = f"https://cefdata.com/funds/{ticker}"
    data = fetch_data_from_url(second_url)
    if data:  # Add the data only if it's not None
        data["Ticker"] = ticker  # Also save the ticker for reference
        all_data.append(data)
    print(f"Data for {ticker}: {data}")

# Convert list of dictionaries to DataFrame
df = pd.DataFrame(all_data)

# Save DataFrame to Excel file
excel_path = "Cef_Data_Base.xlsx"
df.to_excel(excel_path, index=False)

print(f"Data saved to {excel_path}")
