import re
import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

def parse_date_from_key(key_text):
    """
    Attempts to parse a date in the form (mm/dd/yy) from the key text.
    If no date is found, return a 'very old' date so that if there's another
    dated entry, that will supersede this one.
    """
    match = re.search(r'\((\d{1,2}/\d{1,2}/\d{2})\)', key_text)
    if match:
        date_str = match.group(1)
        # Parse date in mm/dd/yy format (e.g. 10/31/24)
        return datetime.strptime(date_str, '%m/%d/%y')
    else:
        # Return a very old date if none is found
        return datetime(1900, 1, 1)

def fetch_data_from_url(url, max_retries=5, backoff_factor=1):
    """
    Returns: (fund_data, permanent_failure)
      - fund_data is either a dict of extracted fields or None
      - permanent_failure is True if a 404 error was encountered (no point retrying)
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Temporary storage for dated fields
            earnings_dict = {}
            unii_dict = {}

            # Other direct fields
            final_data = {}

            elements = soup.find_all('td')
            for i in range(len(elements) - 1):
                key = elements[i].text.strip()
                val = elements[i+1].text.strip()

                if "UNII / Share" in key:
                    date_obj = parse_date_from_key(key)
                    unii_dict[date_obj] = val
                elif "Earnings / Share" in key:
                    date_obj = parse_date_from_key(key)
                    earnings_dict[date_obj] = val
                elif key in [
                    "Current Distribution", "Earn Coverage", "Duration", "Maturity",
                    "Rel Lev Cost", "Outstanding Shares", "Estimated Total Assets",
                    "Total Leverage", "Average Discount (3 Yr)", "Market Yield",
                    "Div Growth (3yr)", "Credit Rating (rbo)", "AMT", "Expense Ratio"
                ]:
                    final_data[key] = val

            # Pick the most recent dated values
            if earnings_dict:
                latest = max(earnings_dict)
                final_data["Earnings / Share"] = earnings_dict[latest]
            if unii_dict:
                latest = max(unii_dict)
                final_data["UNII / Share"] = unii_dict[latest]

            return (final_data if final_data else None), False

        elif response.status_code == 404:
            print(f"404 error for {url}. Permanent failure.")
            return None, True
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
            return None, False

    except Exception as e:
        print(f"Exception while fetching {url}: {e}")
        return None, False


# ------------------- MAIN SCRIPT STARTS HERE -------------------

# List of tickers
tickers = [
     "BNY",
    "ENX",
    "MHN",
    "MYN",
    "NAN",
    "NNY",
    "NRK",
    "NXN",
    "PNI",
    "VTN"							

]

all_data = []
permanent_failed = []
temp_failed = []

# First pass
for ticker in tickers:
    url = f"https://cefdata.com/funds/{ticker}"
    data, permanent = fetch_data_from_url(url)
    if data:
        data["Ticker"] = ticker
        all_data.append(data)
        print(f"Data for {ticker}: {data}")
    else:
        if permanent:
            print(f"Permanent failure for {ticker}.")
            permanent_failed.append(ticker)
        else:
            print(f"No data returned for {ticker}.")
            temp_failed.append(ticker)
    time.sleep(random.uniform(5, 15))

# Retry logic
max_rounds = 3
round_number = 1
failed_tickers = temp_failed

while failed_tickers and round_number <= max_rounds:
    print(f"\n--- Retry Round {round_number} for tickers: {failed_tickers} ---")
    current_failures = []
    for ticker in failed_tickers:
        url = f"https://cefdata.com/funds/{ticker}"
        data, permanent = fetch_data_from_url(url)
        if data:
            data["Ticker"] = ticker
            all_data.append(data)
            print(f"Data for {ticker} fetched on retry {round_number}: {data}")
        else:
            if permanent:
                print(f"Permanent failure detected for {ticker} on retry {round_number}.")
                permanent_failed.append(ticker)
            else:
                current_failures.append(ticker)
        time.sleep(random.uniform(5, 15))
    failed_tickers = current_failures
    round_number += 1

# Combine final failed list
final_failed = permanent_failed + failed_tickers

# Convert to DataFrames
df_success = pd.DataFrame(all_data)
df_failed = pd.DataFrame(final_failed, columns=["Ticker"])

# Write to Excel
excel_path = "Cef_Data_Base.xlsx"
with pd.ExcelWriter(excel_path) as writer:
    df_success.to_excel(writer, sheet_name="Success", index=False)
    df_failed.to_excel(writer, sheet_name="Failed", index=False)

print(f"\nData saved to {excel_path}")
