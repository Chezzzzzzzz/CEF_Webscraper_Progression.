# Import necessary libraries
import requests  # Used for making HTTP requests to get the webpage content
from bs4 import BeautifulSoup  # Used for parsing the HTML content of the webpage
import pandas as pd  # Used for data manipulation and saving data to Excel
import time  # Used to add delays in the script when retrying failed requests

# Function to fetch data from a URL
def fetch_data_from_url(url, delay=10):
    # Setting headers for the request to avoid being blocked by the website
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # While loop to keep trying to fetch data if request fails
    while True:  # Keep trying indefinitely
        try:
            # Making the GET request to fetch the content from the URL
            response = requests.get(url, headers=headers)
            
            # If the request is successful (status code 200), parse the HTML
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')  # Parse HTML using BeautifulSoup
                fund_data = {}  # Initialize a dictionary to store data
                
                # Find all <td> elements in the parsed HTML (table data)
                elements = soup.find_all('td')
                
                # Loop through the elements and collect data
                for i in range(len(elements) - 1):  # Avoid out-of-range error
                    key = elements[i].text.strip()  # Extract text from each <td> and strip surrounding whitespace
                    
                    # Here, the script is trying to match the specific data we care about,
                    # but it looks like there's an incomplete check for specific keys.
                    # The check should be implemented correctly for extracting specific fund data.
                    "UNII / Share" in key or "Earnings / Share" in key or key in ["Average Discount (3 Yr)", "Market Yield", 
                       "Current Distribution", "Div Growth (3yr)", "Earn Coverage"]

                # If fund_data is populated with data, return it
                if fund_data:  # Successfully fetched data
                    return fund_data

            else:
                # If the response status is not 200, print an error and wait before retrying
                print(f"Failed to fetch data, status code: {response.status_code}. Retrying in {delay} seconds...")
                time.sleep(delay)

        except Exception as e:
            # If an exception occurs (e.g., network error), print the error and retry after a delay
            print(f"Error fetching data from {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)

# List of tickers for which data is to be fetched
tickers = ["KTF", "MAV", "MHI"]

# Prepare a list to collect data for all tickers
all_data = []  # This will hold all the data for each ticker

# Loop through each ticker to fetch data
for ticker in tickers:
    second_url = f"https://cefdata.com/funds/{ticker}"  # Construct the URL for each ticker
    data = fetch_data_from_url(second_url)  # Fetch data using the function defined above
    
    # If data was successfully fetched, add the ticker as a reference
    if data:  # Add the data only if it's not None
        data["Ticker"] = ticker  # Save the ticker in the data dictionary for reference
        all_data.append(data)  # Add the data to the list of all data
    
    # Print the fetched data for each ticker
    print(f"Data for {ticker}: {data}")

# Convert list of dictionaries to DataFrame
df = pd.DataFrame(all_data)  # Convert the collected data to a Pandas DataFrame for easy manipulation

# Save the DataFrame to an Excel file
excel_path = "Cef_Data_Base.xlsx"  # Specify the path to save the Excel file
df.to_excel(excel_path, index=False)  # Save the DataFrame to Excel without row indices

# Print confirmation that the data has been saved
print(f"Data saved to {excel_path}")
