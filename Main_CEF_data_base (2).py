# Import necessary libraries
import re  # Used for regular expression operations to search for specific patterns (like dates)
import time  # Used for adding delays (e.g., when retrying failed requests)
import random  # Used to generate random numbers for varying sleep intervals between retries
import requests  # Used for making HTTP requests to fetch webpage content
from bs4 import BeautifulSoup  # Used for parsing and extracting data from HTML content
import pandas as pd  # Used for data manipulation and saving data to Excel
from requests.adapters import HTTPAdapter  # Used to configure HTTP request retries
from urllib3.util.retry import Retry  # Used for setting retry behavior for HTTP requests
from datetime import datetime  # Used to work with dates and times

# Function to parse a date from a given key text
def parse_date_from_key(key_text):
    """
    Attempts to parse a date in the form (mm/dd/yy) from the key text.
    If no date is found, return a 'very old' date so that if there's another
    dated entry, that will supersede this one.
    """
    # Regular expression to search for date in (mm/dd/yy) format
    match = re.search(r'\((\d{1,2}/\d{1,2}/\d{2})\)', key_text)
    if match:
        date_str = match.group(1)  # Extract the matched date string
        # Parse the date string into a datetime object
        return datetime.strptime(date_str, '%m/%d/%y')
    else:
        # Return a very old date if no date is found
        return datetime(1900, 1, 1)

# Function to fetch data from a URL with retry mechanism
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

    # Setting up the session with retry configuration
    session = requests.Session()
    retries = Retry(
        total=max_retries,  # Maximum number of retries
        backoff_factor=backoff_factor,  # Factor by which the delay increases after each failure
        status_forcelist=[500, 502, 503, 504],  # Retry for these status codes
        raise_on_status=False  # Don't raise an exception on error status codes
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        # Send the GET request to fetch content from the URL
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            # Parse the HTML content of the response
            soup = BeautifulSoup(response.text, 'html.parser')

            # Temporary storage for dated fields
            earnings_dict = {}
            unii_dict = {}

            # Other direct fields for final data
            final_data = {}

            # Loop through all <td> elements in the HTML (table data)
            elements = soup.find_all('td')
            for i in range(len(elements) - 1):
                key = elements[i].text.strip()  # Extract the text from the current <td>
                val = elements[i+1].text.strip()  # Extract the text from the next <td>

                # Check for specific keys and extract data accordingly
                if "UNII / Share" in key:
                    date_obj = parse_date_from_key(key)  # Parse the date if found
                    unii_dict[date_obj] = val  # Store value in unii_dict with the date as key
                elif "Earnings / Share" in key:
                    date_obj = parse_date_from_key(key)
                    earnings_dict[date_obj] = val  # Store value in earnings_dict
                elif key in [
                    "Current Distribution", "Earn Coverage", "Duration", "Maturity",
                    "Rel Lev Cost", "Outstanding Shares", "Estimated Total Assets",
                    "Total Leverage", "Average Discount (3 Yr)", "Market Yield",
                    "Div Growth (3yr)", "Credit Rating (rbo)", "AMT", "Expense Ratio"
                ]:
                    final_data[key] = val  # Directly store relevant fields in final_data

            # Pick the most recent dated values
            if earnings_dict:
                latest = max(earnings_dict)  # Get the most recent date from earnings_dict
                final_data["Earnings / Share"] = earnings_dict[latest]  # Store the most recent value
            if unii_dict:
                latest = max(unii_dict)
                final_data["UNII / Share"] = unii_dict[latest]  # Store the most recent UNII value

            # Return the final data if available, and False for permanent failure
            return (final_data if final_data else None), False

        elif response.status_code == 404:
            print(f"404 error for {url}. Permanent failure.")
            return None, True  # Return True for permanent failure in case of 404 error
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
            return None, False  # Retryable failure for other status codes

    except Exception as e:
        print(f"Exception while fetching {url}: {e}")
        return None, False  # Retryable failure for any other exception

# ------------------- MAIN SCRIPT STARTS HERE -------------------

# List of tickers to fetch data for
tickers = [
    "BNY", "ENX", "MHN", "MYN", "NAN", "NNY", "NRK", "NXN", "PNI", "VTN"
]

# Initialize lists to store data and failed tickers
all_data = []  # Successful data
permanent_failed = []  # Tickers that permanently failed (e.g., 404 error)
temp_failed = []  # Tickers that failed temporarily and will be retried

# First pass: Fetch data for each ticker
for ticker in tickers:
    url = f"https://cefdata.com/funds/{ticker}"  # Construct the URL for each ticker
    data, permanent = fetch_data_from_url(url)  # Fetch the data using the function
    if data:
        data["Ticker"] = ticker  # Add ticker to the data
        all_data.append(data)  # Add the data to the success list
        print(f"Data for {ticker}: {data}")
    else:
        if permanent:
            print(f"Permanent failure for {ticker}.")
            permanent_failed.append(ticker)  # Add to permanent failures if it was a 404
        else:
            print(f"No data returned for {ticker}.")
            temp_failed.append(ticker)  # Add to temporary failures
    time.sleep(random.uniform(5, 15))  # Random sleep time between requests to avoid throttling

# Retry logic: Retry fetching data for failed tickers up to 3 times
max_rounds = 3
round_number = 1
failed_tickers = temp_failed  # Start with the temporary failures

while failed_tickers and round_number <= max_rounds:
    print(f"\n--- Retry Round {round_number} for tickers: {failed_tickers} ---")
    current_failures = []  # List of tickers that failed in this round
    for ticker in failed_tickers:
        url = f"https://cefdata.com/funds/{ticker}"
        data, permanent = fetch_data_from_url(url)  # Retry fetching the data
        if data:
            data["Ticker"] = ticker
            all_data.append(data)  # Add successful data
            print(f"Data for {ticker} fetched on retry {round_number}: {data}")
        else:
            if permanent:
                print(f"Permanent failure detected for {ticker} on retry {round_number}.")
                permanent_failed.append(ticker)  # Add to permanent failures if it was a 404
            else:
                current_failures.append(ticker)  # Add to temporary failures for further retrying
        time.sleep(random.uniform(5, 15))  # Random sleep between retries
    failed_tickers = current_failures  # Update list of failed tickers
    round_number += 1  # Move to the next round

# Combine all final failed tickers (permanent + those that still failed after retries)
final_failed = permanent_failed + failed_tickers

# Convert the successful data and failed tickers into Pandas DataFrames
df_success = pd.DataFrame(all_data)
df_failed = pd.DataFrame(final_failed, columns=["Ticker"])

# Write both success and failed data to an Excel file
excel_path = "Cef_Data_Base.xlsx"
with pd.ExcelWriter(excel_path) as writer:
    df_success.to_excel(writer, sheet_name="Success", index=False)  # Save successful data
    df_failed.to_excel(writer, sheet_name="Failed", index=False)  # Save failed tickers

print(f"\nData saved to {excel_path}")
