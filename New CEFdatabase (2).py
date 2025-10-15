# pip install pandas playwright bs4 lxml requests openpyxl tenacity
# python -m playwright install chromium

import re, time, unicodedata
from typing import Dict, List
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://cefdata.com"
TICKERS = ['BFZ', 
    'CEV', 
    'EVM', 
    'MUC', 
    'NAC', 
    'NCA', 
    'NKX', 
    'NXC', 
    'PCK', 
    'PCQ', 
    'PZC', 
    'VCV' ]
OUT_XLSX = "Cef_Data_Base.xlsx"
DEBUG_SAVE_HTML = True  # set False after it works

# Canonical labels you want
TARGET_LABELS = {
    "UNII / Share",
    "Earnings / Share",
    "Current Distribution",
    "Earn Coverage",
    "Duration",
    "Maturity",
    "Relative Leverage Cost",
    "Number of Shares Outstanding",
    "Estimated Total Assets",
    "Total Leverage Ratio",
    "Average Discount (1 Yr)",
    "Distribution Rate based on Market Price",
    "Dividend Growth (3 Yr)",
    "Credit Rating (Rated Bonds Only)",
    "AMT",
    "Expense Ratio",
}

# Variants -> canonical
ALIASES = {
    "UNII per Share": "UNII / Share",
    "UNII/Share": "UNII / Share",
    "Earnings per Share": "Earnings / Share",
    "Earnings/Share": "Earnings / Share",
    "Rel Lev Cost": "Relative Leverage Cost",
    "Rel Lev. Cost": "Relative Leverage Cost",
    "Outstanding Shares": "Number of Shares Outstanding",
    "Total Leverage": "Total Leverage Ratio",
    "Average Discount (3 Yr)": "Average Discount (1 Yr)",
    "Avg Discount (3Yr)": "Average Discount (1 Yr)",
    "Market Yield": "Distribution Rate based on Market Price",
    "Div Growth (3yr)": "Dividend Growth (3 Yr)",
    "Dividend Growth (3 yr)": "Dividend Growth (3 Yr)",
    "Credit Rating (rbo)": "Credit Rating (Rated Bonds Only)",
    "Credit Rating": "Credit Rating (Rated Bonds Only)",
}

# Regex patterns keyed by canonical label (more resilient than exact td/dt matches)
PATTERNS = {
    "UNII / Share": r"\bUNII\s*/\s*Share\b",
    "Earnings / Share": r"\bEarnings\s*/\s*Share\b",
    "Current Distribution": r"\bCurrent\s+Distribution\b",
    "Earn Coverage": r"\bEarn\s+Coverage\b",
    "Duration": r"\bDuration\b",
    "Maturity": r"\bMaturity\b",
    "Relative Leverage Cost": r"\b(Relative\s+Leverage\s+Cost|Rel\s+Lev\s*\.?\s*Cost)\b",
    "Number of Shares Outstanding": r"\bNumber\s+of\s+Shares\s+Outstanding\b|\bOutstanding\s+Shares\b",
    "Estimated Total Assets": r"\bEstimated\s+Total\s+Assets\b",
    "Total Leverage Ratio": r"\bTotal\s+Leverage\s+Ratio\b|\bTotal\s+Leverage\b",
    "Average Discount (1 Yr)": r"\bAverage\s+Discount\s*\(\s*1\s*Yr\s*\)\b|\bAverage\s+Discount\s*\(\s*3\s*Yr\s*\)",
    "Distribution Rate based on Market Price": r"\bDistribution\s+Rate\s+based\s+on\s+Market\s+Price\b|\bMarket\s+Yield\b",
    "Dividend Growth (3 Yr)": r"\bDividend\s+Growth\s*\(\s*3\s*Yr\s*\)\b|\bDiv\s+Growth\s*\(\s*3yr\s*\)",
    "Credit Rating (Rated Bonds Only)": r"\bCredit\s+Rating\s*\(Rated\s+Bonds\s+Only\)\b|\bCredit\s+Rating\b|\(rbo\)",
    "AMT": r"\bAMT\b",
    "Expense Ratio": r"\bExpense\s+Ratio\b",
}

def canon_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def strip_label_dates(label: str) -> str:
    # e.g. "UNII / Share (07/31/2025)" -> "UNII / Share"
    return re.sub(r"\s*\(\d{2}/\d{2}/\d{4}\)\s*$", "", label)

def to_canonical(label: str) -> str:
    label = canon_text(strip_label_dates(label))
    if label in TARGET_LABELS:
        return label
    return ALIASES.get(label, label)

def render_with_playwright(url: str) -> str:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ))
        page = context.new_page()
        page.set_default_timeout(30000)
        # warm the gate first
        gate = f"{BASE}/ch/{url.rsplit('/',2)[-2]}/"
        try:
            page.goto(gate)
        except Exception:
            pass
        time.sleep(0.5)
        page.goto(url)
        # wait for any likely text
        wait_texts = [
            "Number of Shares Outstanding",
            "Expense Ratio",
            "Current Distribution",
            "Distribution Rate based on Market Price",
        ]
        for t in wait_texts:
            try:
                page.get_by_text(t, exact=False).first.wait_for(timeout=5000)
                break
            except Exception:
                continue
        html = page.content()
        context.close()
        browser.close()
        return html

def find_value_near_label(soup: BeautifulSoup, pattern: str) -> str | None:
    # Find a node whose text matches the label, then extract the nearest following cell/value
    node = soup.find(string=re.compile(pattern, re.I))
    if not node:
        return None
    el = node.parent if hasattr(node, "parent") else None
    if not el:
        return None

    # Common structures: <tr><td>Label</td><td>Value</td></tr>
    if el.name in ("td", "th"):
        # sibling cell
        sib = el.find_next_sibling(["td", "th"])
        if sib:
            v = canon_text(sib.get_text(" ", strip=True))
            if v:
                return v
        # or parent row next cell
        row = el.find_parent("tr")
        if row:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                v = canon_text(cells[1].get_text(" ", strip=True))
                if v:
                    return v

    # Definition list: <dt>Label</dt><dd>Value</dd>
    if el.name == "dt":
        dd = el.find_next_sibling("dd")
        if dd:
            v = canon_text(dd.get_text(" ", strip=True))
            if v:
                return v

    # Generic: next visible text block
    nxt = el.find_next(string=True)
    tries = 0
    while nxt and tries < 8:
        v = canon_text(str(nxt))
        if v and not re.search(pattern, v, re.I):
            return v
        nxt = nxt.next_element
        tries += 1
    return None

def parse_html(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    out: Dict[str, str] = {}
    # table/dl fast path
    for tr in soup.select("tr"):
        tds = tr.find_all("td")
        if len(tds) == 2:
            k = to_canonical(tds[0].get_text(strip=True))
            v = canon_text(tds[1].get_text(" ", strip=True))
            if k in TARGET_LABELS and v:
                out[k] = v
    for dl in soup.select("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            k = to_canonical(dt.get_text(strip=True))
            v = canon_text(dd.get_text(" ", strip=True))
            if k in TARGET_LABELS and v:
                out[k] = v
    # regex fallback per label
    for canon_label, pat in PATTERNS.items():
        if canon_label in out:
            continue
        v = find_value_near_label(soup, pat)
        if v:
            out[canon_label] = v
    return out

def scrape_one(ticker: str) -> Dict[str, str]:
    url = f"{BASE}/funds/{ticker.lower()}/"
    html = render_with_playwright(url)  # force JS render for reliability
    if DEBUG_SAVE_HTML:
        with open(f"debug_{ticker}.html", "w", encoding="utf-8") as f:
            f.write(html)
    data = parse_html(html)
    data["Ticker"] = ticker
    return data

def main():
    rows = []
    for t in TICKERS:
        try:
            row = scrape_one(t)
        except Exception as e:
            row = {"Ticker": t, "error": str(e)}
        print(row)
        rows.append(row)

    df = pd.DataFrame(rows)
    # ensure columns
    for col in ["Ticker", *sorted(TARGET_LABELS)]:
        if col not in df.columns:
            df[col] = None
    df = df[["Ticker", *sorted(TARGET_LABELS)] + [c for c in df.columns if c not in TARGET_LABELS and c != "Ticker"]]
    df.to_excel(OUT_XLSX, index=False)
    print(f"Saved -> {OUT_XLSX}")

if __name__ == "__main__":
    main()
