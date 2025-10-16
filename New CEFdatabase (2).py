# Import necessary libraries
import re, time, unicodedata  # Used for regular expression, time delays, and Unicode normalization
from typing import Dict, List  # Used for type hinting (specifying dictionary and list types)
import pandas as pd  # Used for data manipulation and saving to Excel
from bs4 import BeautifulSoup  # Used for parsing HTML content
from playwright.sync_api import sync_playwright  # Used for automating web scraping via Playwright

# Base URL and tickers to scrape
BASE = "https://cefdata.com"
TICKERS = ['BFZ', 'CEV', 'EVM', 'MUC', 'NAC', 'NCA', 'NKX', 'NXC', 'PCK', 'PCQ', 'PZC', 'VCV']
OUT_XLSX = "Cef_Data_Base.xlsx"  # Output Excel file name
DEBUG_SAVE_HTML = True  # Whether to save the HTML for debugging purposes (set False after it works)

# Canonical labels we want to extract from the page
TARGET_LABELS = {
    "UNII / Share", "Earnings / Share", "Current Distribution", "Earn Coverage", "Duration", "Maturity",
    "Relative Leverage Cost", "Number of Shares Outstanding", "Estimated Total Assets", "Total Leverage Ratio",
    "Average Discount (1 Yr)", "Distribution Rate based on Market Price", "Dividend Growth (3 Yr)", 
    "Credit Rating (Rated Bonds Only)", "AMT", "Expense Ratio",
}

# Alias labels to standardize variations in text
ALIASES = {
    "UNII per Share": "UNII / Share", "UNII/Share": "UNII / Share", "Earnings per Share": "Earnings / Share", 
    "Earnings/Share": "Earnings / Share", "Rel Lev Cost": "Relative Leverage Cost", "Outstanding Shares": "Number of Shares Outstanding",
    "Total Leverage": "Total Leverage Ratio", "Avg Discount (3Yr)": "Average Discount (1 Yr)", "Market Yield": "Distribution Rate based on Market Price",
    "Div Growth (3yr)": "Dividend Growth (3 Yr)", "Credit Rating": "Credit Rating (Rated Bonds Only)",
}

# Regex patterns for matching key text in HTML elements
PATTERNS = {
    "UNII / Share": r"\bUNII\s*/\s*Share\b", "Earnings / Share": r"\bEarnings\s*/\s*Share\b",
    "Current Distribution": r"\bCurrent\s+Distribution\b", "Earn Coverage": r"\bEarn\s+Coverage\b",
    "Duration": r"\bDuration\b", "Maturity": r"\bMaturity\b", "Relative Leverage Cost": r"\b(Relative\s+Leverage\s+Cost|Rel\s+Lev\s*\.?\s*Cost)\b",
    "Number of Shares Outstanding": r"\bNumber\s+of\s+Shares\s+Outstanding\b|\bOutstanding\s+Shares\b", 
    "Estimated Total Assets": r"\bEstimated\s+Total\s+Assets\b", "Total Leverage Ratio": r"\bTotal\s+Leverage\s+Ratio\b|\bTotal\s+Leverage\b", 
    "Average Discount (1 Yr)": r"\bAverage\s+Discount\s*\(\s*1\s*Yr\s*\)\b|\bAverage\s+Discount\s*\(\s*3\s*Yr\s*\)", 
    "Distribution Rate based on Market Price": r"\bDistribution\s+Rate\s+based\s+on\s+Market\s+Price\b|\bMarket\s+Yield\b",
    "Dividend Growth (3 Yr)": r"\bDividend\s+Growth\s*\(\s*3\s*Yr\s*\)\b|\bDiv\s+Growth\s*\(\s*3yr\s*\)", 
    "Credit Rating (Rated Bonds Only)": r"\bCredit\s+Rating\s*\(Rated\s+Bonds\s+Only\)\b|\bCredit\s+Rating\b|\(rbo\)", 
    "AMT": r"\bAMT\b", "Expense Ratio": r"\bExpense\s+Ratio\b",
}

# Normalize and standardize the text
def canon_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "").strip()  # Normalize Unicode characters
    s = re.sub(r"\s+", " ", s)  # Replace consecutive spaces with a single space
    return s

# Strip the date from labels like "UNII / Share (07/31/2025)" -> "UNII / Share"
def strip_label_dates(label: str) -> str:
    return re.sub(r"\s*\(\d{2}/\d{2}/\d{4}\)\s*$", "", label)

# Convert labels to canonical form using the defined aliases
def to_canonical(label: str) -> str:
    label = canon_text(strip_label_dates(label))
    if label in TARGET_LABELS:
        return label
    return ALIASES.get(label, label)

# Use Playwright to render the page and get the HTML content
def render_with_playwright(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ))
        page = context.new_page()
        page.set_default_timeout(30000)
        
        # Try warming the page first
        gate = f"{BASE}/ch/{url.rsplit('/',2)[-2]}/"
        try:
            page.goto(gate)
        except Exception:
            pass
        time.sleep(0.5)  # Small wait to make sure the page loads
        page.goto(url)  # Navigate to the final page URL
        # Wait for key texts to appear on the page
        wait_texts = [
            "Number of Shares Outstanding", "Expense Ratio", "Current Distribution", 
            "Distribution Rate based on Market Price",
        ]
        for t in wait_texts:
            try:
                page.get_by_text(t, exact=False).first.wait_for(timeout=5000)
                break
            except Exception:
                continue
        html = page.content()  # Get the HTML content after rendering
        context.close()
        browser.close()
        return html

# Find the value near a given label (uses regex patterns)
def find_value_near_label(soup: BeautifulSoup, pattern: str) -> str | None:
    node = soup.find(string=re.compile(pattern, re.I))  # Find a node that matches the pattern
    if not node:
        return None
    el = node.parent if hasattr(node, "parent") else None
    if not el:
        return None
    
    # Check common structures for matching: <tr><td>Label</td><td>Value</td>
    if el.name in ("td", "th"):
        sib = el.find_next_sibling(["td", "th"])
        if sib:
            v = canon_text(sib.get_text(" ", strip=True))
            if v:
                return v
        # Or try to get the next sibling in the row
        row = el.find_parent("tr")
        if row:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                v = canon_text(cells[1].get_text(" ", strip=True))
                if v:
                    return v

    # Definition list structure: <dt>Label</dt><dd>Value</dd>
    if el.name == "dt":
        dd = el.find_next_sibling("dd")
        if dd:
            v = canon_text(dd.get_text(" ", strip=True))
            if v:
                return v

    # Generic fallback: Next visible text block
    nxt = el.find_next(string=True)
    tries = 0
    while nxt and tries < 8:
        v = canon_text(str(nxt))
        if v and not re.search(pattern, v, re.I):
            return v
        nxt = nxt.next_element
        tries += 1
    return None

# Parse the HTML content and extract key data
def parse_html(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")  # Parse the HTML using BeautifulSoup and lxml parser
    out: Dict[str, str] = {}
    # Fast path for extracting data from <tr><td> and <dl><dt> elements
    for tr in soup.select("tr"):
        tds = tr.find_all("td")
        if len(tds) == 2:
            k = to_canonical(tds[0].get_text(strip=True))  # Standardize label
            v = canon_text(tds[1].get_text(" ", strip=True))  # Get value and normalize
            if k in TARGET_LABELS and v:
                out[k] = v
    for dl in soup.select("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            k = to_canonical(dt.get_text(strip=True))
            v = canon_text(dd.get_text(" ", strip=True))
            if k in TARGET_LABELS and v:
                out[k] = v
    # Fallback: Use regex patterns to extract data for missing labels
    for canon_label, pat in PATTERNS.items():
        if canon_label in out:
            continue
        v = find_value_near_label(soup, pat)
        if v:
            out[canon_label] = v
    return out

# Scrape data for a single ticker
def scrape_one(ticker: str) -> Dict[str, str]:
    url = f"{BASE}/funds/{ticker.lower()}/"  # Construct URL for the specific ticker
    html = render_with_playwright(url)  # Use Playwright to render the page and get the HTML
    if DEBUG_SAVE_HTML:
        # Save the raw HTML for debugging purposes
        with open(f"debug_{ticker}.html", "w", encoding="utf-8") as f:
            f.write(html)
    data = parse_html(html)  # Parse the HTML and extract relevant data
    data["Ticker"] = ticker  # Add ticker to the data
    return data

# Main function to scrape all tickers and save data to an Excel file
def main():
    rows = []
    for t in TICKERS:
        try:
            row = scrape_one(t)  # Scrape data for each ticker
        except Exception as e:
            row = {"Ticker": t, "error": str(e)}  # Handle any errors during scraping
        print(row)
        rows.append(row)

    # Create a DataFrame and ensure columns are in the right order
    df = pd.DataFrame(rows)
    for col in ["Ticker", *sorted(TARGET_LABELS)]:
        if col not in df.columns:
            df[col] = None
    df = df[["Ticker", *sorted(TARGET_LABELS)] + [c for c in df.columns if c not in TARGET_LABELS and c != "Ticker"]]
    df.to_excel(OUT_XLSX, index=False)  # Save the DataFrame to an Excel file
    print(f"Saved -> {OUT_XLSX}")

# Execute the main function if the script is run directly
if __name__ == "__main__":
    main()
