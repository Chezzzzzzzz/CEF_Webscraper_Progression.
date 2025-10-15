#!/usr/bin/env python3
import time, re, csv, requests, datetime

UA = {"User-Agent": "YourName your.email@example.com"}  # REQUIRED by SEC
TICKERS = []  # edit your tickers here
FORMS = {"8-K","25","25-NSE","15-12B","15-12G","15-15D","S-4","F-4",
         "DEFM14A","PREM14A","SC 13E3","SC 13E-3","TO-T","TO-I","14D-9",
         "6-K","N-8F","497"}

KW = re.compile(r"(merger|agreement and plan of merger|plan of merger|going ?private|"
                r"acquisition|tender offer|cash merger|take-?private|"
                r"plan of liquidation|plan of dissolution|liquidat|wind down|"
                r"delist|deregist|chapter 11|bankruptcy|receivership)", re.I)

CUTOFF = datetime.date.today() - datetime.timedelta(days=90)

def map_ticker_to_cik():
    m = requests.get("https://www.sec.gov/files/company_tickers.json", headers=UA, timeout=30).json()
    return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in m.values()}

def submissions(cik):
    return requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=UA, timeout=30).json()

def fetch_doc(cik, acc_no, primary_doc):
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no.replace('-','')}/{primary_doc}"
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r.text

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
    flags["deal_closed"] = bool(re.search(r"item\s*2\.01|completion of (the )?merger|closing of the merger", html, re.I))
    return flags

def scan(tickers):
    tic2cik = map_ticker_to_cik()
    out = []
    for t in tickers:
        cik = tic2cik.get(t.upper())
        if not cik:
            out.append({"ticker": t, "note": "CIK not found"}); continue
        j = submissions(cik)
        r = j.get("filings", {}).get("recent", {})
        forms = r.get("form", []); dates = r.get("filingDate", [])
        accs  = r.get("accessionNumber", []); docs = r.get("primaryDocument", [])
        for i, f in enumerate(forms):
            # date cutoff
            try:
                fdate = datetime.date.fromisoformat(dates[i])
            except Exception:
                continue
            if fdate < CUTOFF:
                continue
            if f not in FORMS:
                continue
            rec = {"ticker": t.upper(),"cik": cik,"form": f,"date": dates[i],
                   "accession": accs[i],"primary_document": docs[i]}
            try:
                html = fetch_doc(cik, accs[i], docs[i])
                if f == "8-K" or KW.search(html):
                    rec.update(classify(f, html))
            except Exception as e:
                rec["error"] = str(e)[:120]
            out.append(rec)
            time.sleep(0.25)
        time.sleep(0.25)
    return out

def risk_state(row):
    if row.get("delisted"): return "DELISTED"
    if row.get("deregistration"): return "DEREGISTERING"
    if row.get("bankruptcy"): return "BANKRUPTCY"
    if row.get("deal_closed"): return "MERGER CLOSED"
    if row.get("deal_announced") or row.get("tender_offer") or row.get("going_private"): return "TRANSACTION ANNOUNCED"
    if row.get("liquidation"): return "LIQUIDATION PLAN"
    if row.get("delist_notice"): return "DELIST NOTICE"
    return ""

def write_csv(rows, path="tradeability_risk_events.csv"):
    if not rows: return
    fields = sorted({k for r in rows for k in r.keys()}.union({"state"}))
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows:
            r["state"] = risk_state(r); w.writerow(r)

if __name__ == "__main__":
    rows = scan(TICKERS)
    write_csv(rows)
    print("Wrote tradeability_risk_events.csv")

