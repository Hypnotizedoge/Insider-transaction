"""
Bursa Malaysia - Scraper for "Dealings in Listed Securities" (Chapter 14)
Target: https://www.bursamalaysia.com/bm/market_information/announcements/company_announcement?company=0151

Uses cloudscraper to bypass Cloudflare and directly hit the AJAX API.
Extracts from detail pages: Date of Transaction, Name, Price, No. of Shares, Type
"""

import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
COMPANY_CODE  = "0151"          # Bursa company code
CATEGORY_ID   = "14"            # 14 = Dealings in Listed Securities (Chapter 14)
PAGES_TO_SCRAPE = 10             # Number of listing pages to iterate
OUTPUT_CSV    = "bursa_dealings.csv"

BASE = "https://www.bursamalaysia.com"
MAIN_URL = f"{BASE}/bm/market_information/announcements/company_announcement?company={{company}}"
API_URL = f"{BASE}/api/v1/announcements/search?ann_type=company&company={{company}}&category={{category}}&per_page=50&page={{page}}"


def snooze(lo=1.0, hi=2.5):
    time.sleep(random.uniform(lo, hi))


def parse_detail(html_text: str) -> list:
    soup = BeautifulSoup(html_text, "lxml")
    
    NAME_KW   = ("name of director", "name of person", "name of insider", "name of major", "full name", "name of shareholder")
    DATE_KW   = ("date of transaction", "date of dealing", "date of change", "transaction date")
    SHARES_KW = ("no. of securities", "no of securities", "number of shares", "securities acquired", "securities disposed", "quantity", "no. of shares transacted")
    PRICE_KW  = ("price per share", "transaction price", "consideration per", "price (rm", "price(rm", "market price", "consideration")
    TYPE_KW   = ("nature of transaction", "type of transaction", "transaction type", "nature of dealing")
    DESIG_KW  = ("designation", "position", "title")
    DESC_KW   = ("description of securities", "class of securities", "type of securities", "description of security")

    def clean_num(v):
        if v is None: return None
        n = re.sub(r"[^\d.]", "", str(v))
        try: return float(n) if n else None
        except: return None

    def match(label, kws):
        l = label.lower().strip()
        return any(k in l for k in kws)

    # 1. Page-level name detection (found once per page often)
    text_all = soup.get_text("\n")
    page_name = None
    m_name = re.search(r"^(?:Name|Name of (?:director|person|major shareholder|insider|registered holder))[\s:\n|]+([A-Za-z0-9\s.,'()@&-]+?)\n", text_all, re.I | re.MULTILINE)
    if m_name: page_name = m_name.group(1).strip()
    if not page_name:
        m2 = re.search(r"(?:MR|MRS|MS|MADAM|DATO|DATUK|DR|TAN SRI|PUAN|ENCIK)[\s\n]+([A-Za-z\s.,'@-]+?)(?:\n\n|\n\s*\n|\n\s*(?:Director|Principal Officer|Major|Others|Group|CEO|CFO|COO|Secretary))", text_all, re.I)
        if m2: page_name = m2.group(1).strip()

    all_records = []
    
    def new_rec():
        return {"Name": None, "Designation": None, "Description of Securities": None, "Date of Transaction": None, "Price (RM)": None, "No. of Shares": None, "Transaction Type": None}

    # 2. Strategy: Process all tables
    for table in soup.find_all("table"):
        table_recs = []
        rows = table.find_all("tr")
        if not rows: continue
        
        # Horizontal Strategy: Multi-column header
        headers = [h.get_text(strip=True) for h in rows[0].find_all(["th", "td"])]
        if len(headers) > 2 and any(match(h, DATE_KW) for h in headers):
            for data_row in rows[1:]:
                rec = new_rec()
                vals = [td.get_text(strip=True) for td in data_row.find_all("td")]
                for i, h in enumerate(headers):
                    if i >= len(vals): break
                    v = vals[i]
                    if match(h, NAME_KW): rec["Name"] = v
                    elif match(h, DESIG_KW): rec["Designation"] = v
                    elif match(h, DESC_KW): rec["Description of Securities"] = v
                    elif match(h, DATE_KW): rec["Date of Transaction"] = v
                    elif match(h, SHARES_KW): rec["No. of Shares"] = clean_num(v)
                    elif match(h, PRICE_KW):
                        m_pr = re.search(r"([\d,]+\.?\d*)", v)
                        if m_pr: rec["Price (RM)"] = clean_num(m_pr.group(1))
                    elif match(h, TYPE_KW): rec["Transaction Type"] = v
                if rec["Date of Transaction"] or rec["No. of Shares"]:
                    table_recs.append(rec)
        else:
            # Vertical/Stacked Strategy: 2-column label-value, but multiple blocks
            cur_rec = None
            for tr in rows:
                cells = tr.find_all(["td", "th"])
                vals = [c.get_text(strip=True) for c in cells]
                if not vals: continue
                
                # Skip the table header row itself
                if len(vals) >= 2 and vals[0].lower() == "no" and vals[1].lower() == "salutation":
                    continue

                # Detect header row of a RECORD (e.g. [1, MR, Name, Designation])
                is_record_start = False
                if len(vals) >= 3 and any(s in vals[1].upper() for s in ["MR", "MRS", "MS", "DATO", "DATUK", "DR", "TAN SRI", "PUAN", "ENCIK"]):
                    is_record_start = True
                
                    if cur_rec and (cur_rec["Date of Transaction"] or cur_rec["No. of Shares"]):
                        table_recs.append(cur_rec)
                    cur_rec = new_rec()
                    cur_rec["Name"] = vals[2]
                    if len(vals) >= 4:
                        d = vals[3]
                        if len(vals) >= 5 and d.lower() == "others":
                            d = vals[4]
                        cur_rec["Designation"] = d
                else:
                    if len(vals) < 2: continue
                    label = vals[0]
                    val = vals[1]
                    
                    if not cur_rec: cur_rec = new_rec()
                    
                    if match(label, NAME_KW) and (not cur_rec["Name"] or cur_rec["Name"] == "Name"): 
                        if val: cur_rec["Name"] = val
                    elif match(label, DESIG_KW) and (not cur_rec["Designation"] or cur_rec["Designation"].lower() == "designation"):
                        if val: cur_rec["Designation"] = val
                    elif match(label, DESC_KW) and (not cur_rec["Description of Securities"] or cur_rec["Description of Securities"].lower() == "description"):
                        if val: cur_rec["Description of Securities"] = val
                    elif match(label, DATE_KW): 
                        if val: cur_rec["Date of Transaction"] = val
                    elif match(label, SHARES_KW): 
                        if val: cur_rec["No. of Shares"] = clean_num(val)
                    elif match(label, PRICE_KW):
                        m_pr = re.search(r"([\d,]+\.?\d*)", val)
                        if m_pr: cur_rec["Price (RM)"] = clean_num(m_pr.group(1))
                    elif match(label, TYPE_KW): 
                        if val: cur_rec["Transaction Type"] = val
                    elif "description of \"others\" type" in label.lower() and val:
                        # Only use 'Others' description if main type is empty or 'others'
                        if not cur_rec["Transaction Type"] or cur_rec["Transaction Type"].lower() == "others":
                            cur_rec["Transaction Type"] = val
                    elif "description of \"others\" designation" in label.lower() and val:
                        if not cur_rec["Designation"] or cur_rec["Designation"].lower() == "others":
                            cur_rec["Designation"] = val
            
            if cur_rec and (cur_rec["Date of Transaction"] or cur_rec["No. of Shares"]):
                table_recs.append(cur_rec)

        for r in table_recs:
             if not r["Name"]: r["Name"] = page_name
        all_records.extend(table_recs)

    # 3. Global Text Fallback if no tables extracted data
    if not all_records:
        rec = new_rec()
        rec["Name"] = page_name
        m_dt = re.search(r"(?:date of (?:transaction|dealing|change))[\s:\n]+([\d]{1,2}\s+[A-Za-z]+\s+[\d]{4}|\d{1,2}/\d{1,2}/\d{4})", text_all, re.I)
        if m_dt: rec["Date of Transaction"] = m_dt.group(1).strip()
        m_sh = re.search(r"(?:number of shares|[Nn]o\.? of securities|quantity)[\s:\n]+([\d,]+)", text_all, re.I)
        if m_sh: rec["No. of Shares"] = clean_num(m_sh.group(1))
        m_pr = re.search(r"(?:price|consideration)[\sA-Za-z/()]*?[:\r\n\s]+(?:MYR|RM)?\s*([\d,]+\.\d+)", text_all, re.I)
        if m_pr: rec["Price (RM)"] = clean_num(m_pr.group(1))
        m_ty = re.search(r"(?:nature|type) of (?:transaction|interest)[\s:\n]+([A-Za-z\s]+?)\n", text_all, re.I)
        if m_ty: rec["Transaction Type"] = m_ty.group(1).strip()
        m_ds = re.search(r"(?:description|class|type) of securities[\s:\n]+([A-Za-z\s]+?)\n", text_all, re.I)
        if m_ds: rec["Description of Securities"] = m_ds.group(1).strip()
        if any(v for k, v in rec.items() if k != "Name"):
            all_records.append(rec)

    return all_records


def scrape(company_code: str = COMPANY_CODE, category: str = CATEGORY_ID, pages: int = PAGES_TO_SCRAPE) -> pd.DataFrame:
    log.info(f"\n{'='*60}")
    log.info(f"Bursa Dealings Scraper (HTTP API) – Company: {company_code} Category: {category}")
    log.info(f"{'='*60}\n")

    scraper = cloudscraper.create_scraper()
    main_url = MAIN_URL.format(company=company_code)
    
    log.info("[1] Getting Cloudflare clearance…")
    scraper.get(main_url)
    snooze()

    log.info("[2] Collecting announcements via API…")
    all_links = []
    
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': main_url
    }

    for p in range(1, pages + 1):
        url = API_URL.format(company=company_code, category=category, page=p)
        r = scraper.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            log.warning(f"  Error fetching page {p}: HTTP {r.status_code}")
            break
            
        try:
            data = r.json()
            rows = data.get("data", [])
        except Exception as e:
            log.warning(f"  Error parsing JSON on page {p}: {e}")
            break
            
        log.info(f"  Page {p}: API returned {len(rows)} items")
        if not rows:
            break
            
        for row in rows:
            if len(row) > 3:
                soup = BeautifulSoup(row[3], 'html.parser')
                a = soup.find('a', href=True)
                if a:
                    href = a['href']
                    full_url = href if href.startswith("http") else f"{BASE}{href}"
                    title = a.get_text(strip=True)
                    
                    # Try to get date from row[1]
                    date_soup = BeautifulSoup(row[1], 'html.parser')
                    date_text = " ".join(date_soup.stripped_strings)
                    # Deduplicate repeated dates from responsive DOM
                    date_posted = ' '.join(date_text.split()[:3])

                    upper_title = title.upper()
                    if "DEALINGS IN LISTED SECURITIES" in upper_title and "CHAPTER 14" in upper_title:
                        if "INTENTION" not in upper_title and ("DEALINGS OUTSIDE CLOSED PERIOD" in upper_title or "DEALINGS DURING CLOSED PERIOD" in upper_title):
                            all_links.append({"href": full_url, "title": title, "date_posted": date_posted})
        
        snooze()

    log.info(f"\n  Total links found: {len(all_links)}")

    log.info("\n[3] Scraping detail pages…")
    raw_results = []
    for i, lnk in enumerate(all_links, 1):
        href = lnk["href"]
        title = lnk["title"]
        log.info(f"  [{i}/{len(all_links)}] {title[:70]}")

        try:
            r = scraper.get(href, timeout=15)
        except Exception as e:
            log.warning(f"    Request failed: {e}")
            continue
            
        if r.status_code == 200:
            # The actual announcement details are in an iframe
            soup = BeautifulSoup(r.text, 'lxml')
            iframe = soup.find('iframe', id='bm_ann_detail_iframe')
            if iframe and iframe.has_attr('src'):
                iframe_src = iframe['src']
                if not iframe_src.startswith('http'):
                    iframe_src = f"https://www.bursamalaysia.com{iframe_src}"
                
                try:
                    r_iframe = scraper.get(iframe_src, timeout=15)
                    if r_iframe.status_code == 200:
                        detail_list = parse_detail(r_iframe.text)
                        
                        for entry in detail_list:
                            raw_results.append({
                                "Date Posted":        lnk["date_posted"],
                                "Title":              title,
                                "Name":               entry["Name"],
                                "Designation":        entry["Designation"],
                                "Description":        entry["Description of Securities"],
                                "Date of Transaction":entry["Date of Transaction"],
                                "Price (RM)":         entry["Price (RM)"],
                                "No. of Shares":      entry["No. of Shares"],
                                "Transaction Type":   entry["Transaction Type"],
                                "URL":                href,
                            })
                    else:
                        log.warning(f"    Iframe fetch failed: HTTP {r_iframe.status_code}")
                except Exception as e:
                    log.warning(f"    Iframe request failed: {e}")
            else:
                # Fallback to main page parsing if no iframe
                detail_list = parse_detail(r.text)
                for entry in detail_list:
                    raw_results.append({
                        "Date Posted":        lnk["date_posted"],
                        "Title":              title,
                        "Name":               entry["Name"],
                        "Designation":        entry["Designation"],
                        "Description":        entry["Description of Securities"],
                        "Date of Transaction":entry["Date of Transaction"],
                        "Price (RM)":         entry["Price (RM)"],
                        "No. of Shares":      entry["No. of Shares"],
                        "Transaction Type":   entry["Transaction Type"],
                        "URL":                href,
                    })
        else:
            log.warning(f"    Failed to fetch detail page: HTTP {r.status_code}")
        snooze(0.5, 1.5)

    log.info("\n[4] Filtering results…")
    results = []
    for r in raw_results:
        t_type = str(r.get("Transaction Type") or "").lower()
        d_sec  = str(r.get("Description") or "").lower()
        
        # Filter for Acquisitions/Disposals
        is_deal = any(term in t_type for term in ["acquired", "acquisition", "disposed", "disposal", "bought", "sold"])
        
        # Filter for Ordinary Shares: pass if description contains "ordinary share" OR if description is blank/missing
        # (Description is often unparsed; don't silently drop valid dealings)
        is_ordinary = "ordinary share" in d_sec or d_sec.strip() == ""
        
        if is_deal and is_ordinary:
            results.append(r)

    df = pd.DataFrame(results)
    if not df.empty:
        for col in ("Date Posted", "Date of Transaction"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
        df.sort_values("Date Posted", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

    return df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Bursa Malaysia Announcements")
    parser.add_argument("--company", type=str, default=COMPANY_CODE, help="Company code (e.g. 0151)")
    parser.add_argument("--category", type=str, default=CATEGORY_ID, help="Category ID (e.g. 14)")
    parser.add_argument("--pages", type=int, default=PAGES_TO_SCRAPE, help="Pages to scrape")
    parse_args = parser.parse_args()
    
    df = scrape(company_code=parse_args.company, category=parse_args.category, pages=parse_args.pages)

    log.info("\n=== RESULTS ===")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", 40)
    pd.set_option("display.width", 160)

    if not df.empty:
        log.info(df[["Date of Transaction", "Name", "Designation", "Price (RM)", "No. of Shares", "Transaction Type"]].to_string(index=False))
        df.to_csv(OUTPUT_CSV, index=False)
        log.info(f"\n✓ Saved to {OUTPUT_CSV}")
        log.info(f"  Rows: {len(df)}")
    else:
        log.info("No data extracted.")
