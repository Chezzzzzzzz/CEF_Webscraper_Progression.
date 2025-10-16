#!/usr/bin/env python3
# Import necessary libraries
import time, re, csv, requests, datetime  # Standard libraries for time, regex, CSV handling, HTTP requests, and date manipulation

# Define user-agent header for SEC requests
UA = {"User-Agent": "YourName your.email@example.com"}  # REQUIRED by SEC to access their data

# List of tickers to scan (edit as needed)
TICKERS = []  # Placeholder list for tickers

# Forms of interest (these are specific types of SEC filings)
FORMS = {"8-K","25","25-NSE","15-12B","15-12G","15-15D","S-4","F-4",
         "DEFM14A","PREM14A","SC 13E3","SC 13E-3","TO-T","TO-I","14D-9",
         "6-K","N-8F","497"}

# Regular expression to search for relevant keywords in filings (e.g., merger, acquisition, bankruptcy)
KW = re.compile(r"(merger|agreement and plan of merger|plan of merger|going ?private|"
                r"acquisition|tender offer|cash merger|take-?private|"
                r"plan of liquidation|plan of dissolution|liquidat|wind down|"
                r"delist|deregist|chapter 11|bankruptcy|receivership)", re.I)

# Date cutoff for the filings (files from the last 90 days)
CUTOFF = datetime.date.today() - datetime.timedelta(days=90)

# Map tickers to their corresponding CIK (Central Index Key) using SEC's public JSON data
def map_ticker_to_cik():
    m = requests.get("https://www.sec.gov/files/company_tickers.json", headers=UA, timeout=30).json()
    return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in m.values()}

# Get the recent filings for a given CIK
def submissions(cik):
    return requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=UA, timeout=30).json()

# Fetch the specific document using the CIK, accession number, and document name
def fetch_doc(cik, acc_no, primary_doc):
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no.replace('-','')}/{primary_doc}"
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()  # Raise an exception if the request fails
    return r.text  # Return the document content as a string

# Classify the document into categories based on its content (e.g., merger, tender offer, liquidation)
def classify(form, html):
    flags = {
        "deal_announced": bool(re.search(r"(agreement and plan of merger|plan of merger|merger agreement)", html, re.I)),
        "tender_offer": bool(re.search(r"tender offer|14D-9", html, re.I)),
        "going_private": bool(re.search(r"going ?private|13E-3", html, re.I)),
        "liquidation": bool(re.search(r"plan of liquidation|plan of dissolution|liquidat", html, re.I)),
        "bankruptcy": bool(re.search(r"item\s*1\.03|chapter\s*11|bankruptcy|receivership", html, re.I)),
        "delist_notice": bool(re.search(r"item\s*3\.01|delist", html, re.I)),
        "deregistration": form in {"15-12B","15-12G","15-15D"},
        "delisted": form in {"25","25-NSE"}
    }
    # If the merger or transaction has closed
    flags["deal_closed"] = bool(re.search(r"item\s*2\.01|completion of (the )?merger|closing of the merger", html, re.I))
    return flags

# Main scanning function for all tickers
def scan(tickers):
    tic2cik = map_ticker_to_cik()  # Map tickers to CIKs
    out = []
    for t in tickers:
        cik = tic2cik.get(t.upper())  # Get CIK for each ticker
        if not cik:
            out.append({"ticker": t, "note": "CIK not found"}); continue  # Skip tickers without a CIK
        j = submissions(cik)  # Get the recent submissions for the CIK
        r = j.get("filings", {}).get("recent", {})  # Get the recent filings data
        forms = r.get("form", [])
        dates = r.get("filingDate", [])
        accs  = r.get("accessionNumber", [])
        docs = r.get("primaryDocument", [])
        for i, f in enumerate(forms):
            # Date cutoff to limit the range of filings
            try:
                fdate = datetime.date.fromisoformat(dates[i])
            except Exception:
                continue
            if fdate < CUTOFF:  # Skip filings older than the cutoff
                continue
            if f not in FORMS:  # Only process the relevant forms
                continue
            rec = {"ticker": t.upper(), "cik": cik, "form": f, "date": dates[i],
                   "accession": accs[i], "primary_document": docs[i]}
            try:
                html = fetch_doc(cik, accs[i], docs[i])  # Fetch the filing document
                if f == "8-K" or KW.search(html):  # Check if the filing matches our keywords
                    rec.update(classify(f, html))  # Classify the document based on its content
            except Exception as e:
                rec["error"] = str(e)[:120]  # Capture any errors and store them
            out.append(rec)
            time.sleep(0.25)  # Sleep to avoid overloading the SEC server
        time.sleep(0.25)  # Sleep to avoid overloading the SEC server
    return out

# Function to determine the "risk state" based on the flags set in classify()
def risk_state(row):
    if row.get("delisted"): return "DELISTED"
    if row.get("deregistration"): return "DEREGISTERING"
    if row.get("bankruptcy"): return "BANKRUPTCY"
    if row.get("deal_closed"): return "MERGER CLOSED"
    if row.get("deal_announced") or row.get("tender_offer") or row.get("going_private"): return "TRANSACTION ANNOUNCED"
    if row.get("liquidation"): return "LIQUIDATION PLAN"
    if row.get("delist_notice"): return "DELIST NOTICE"
    return ""  # If none of the above, return an empty string

# Function to write the results to a CSV file
def write_csv(rows, path="tradeability_risk_events.csv"):
    if not rows: return  # If no data, don't write anything
    fields = sorted({k for r in rows for k in r.keys()}.union({"state"}))  # Get all the keys (fields) in the data and add 'state'
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)  # Create a CSV writer
        w.writeheader()  # Write the header row
        for r in rows:
            r["state"] = risk_state(r)  # Add the "state" field based on risk_state
            w.writerow(r)  # Write each row of data

# Main execution block
if __name__ == "__main__":
    rows = scan(TICKERS)  # Scan the tickers for filings and classify them
    write_csv(rows)  # Write the results to a CSV file
    print("Wrote tradeability_risk_events.csv")  # Confirm that the CSV file has been written
