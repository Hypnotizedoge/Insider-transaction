"""
Bursa Malaysia - Fast Scraper for "Dealings in Listed Securities" (Chapter 14)
Uses the JSON API to get announcement links, then fetches detail pages in parallel.
"""

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

IMPERSONATE = "chrome124"


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
COMPANY_CODE    = "0001"
CATEGORY_ID     = "DRCO"     # Director/CEO and Major Shareholder Dealings
PAGES_TO_SCRAPE = 5
OUTPUT_CSV      = f"bursa_dealings_{COMPANY_CODE}.csv"
MAX_WORKERS  = 8

BASE     = "https://www.bursamalaysia.com"
MAIN_URL = f"{BASE}/bm/market_information/announcements/company_announcement?company={{company}}"
API_URL  = (
    f"{BASE}/api/v1/announcements/search"
    "?ann_type=company&company={company}&cat={category}&per_page=50&page={page}"
)

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ms-MY,ms;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Origin": "https://www.bursamalaysia.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}






# ──────────────────────────────────────────────────────────────────────────────
# PARSING
# ──────────────────────────────────────────────────────────────────────────────
NAME_KW   = ("name of director", "name of person", "name of insider", "name of major",
             "full name", "name of shareholder")
DATE_KW   = ("date of transaction", "date of dealing", "date of change", "transaction date")
SHARES_KW = ("no. of securities", "no of securities", "number of shares",
             "securities acquired", "securities disposed", "quantity", "no. of shares transacted")
PRICE_KW  = ("price per share", "transaction price", "consideration per",
             "price (rm", "price(rm", "market price", "consideration", "consideration (myr)")

TYPE_KW   = ("nature of transaction", "type of transaction", "transaction type", "nature of dealing", "circumstances", "nature of change")
DESIG_KW  = ("designation", "position", "title")
DESC_KW   = ("description of securities", "class of securities", "type of securities",
             "description of security")


def _clean_num(v):
    if v is None:
        return None
    # Handle cases like "1.20 (avg)" or "RM1.20"
    n = re.sub(r"[^\d.]", "", str(v))
    # If there are multiple dots, take the first one (e.g. 1.20.0 -> 1.20)
    parts = n.split('.')
    if len(parts) > 2:
        n = parts[0] + '.' + "".join(parts[1:])
    try:
        return float(n) if n else None
    except Exception:
        return None



def _match(label, kws):
    l = label.lower().strip()
    return any(k in l for k in kws)


def _new_rec():
    return {
        "Name": None, "Designation": None, "Description of Securities": None,
        "Date of Transaction": None, "Price (RM)": None,
        "No. of Shares": None, "Transaction Type": None,
    }


def parse_detail(html_text: str) -> list:
    soup = BeautifulSoup(html_text, "lxml")
    text_all = soup.get_text("\n")

    # Page-level name detection
    page_name = None
    m = re.search(
        r"^(?:Name|Name of (?:director|person|major shareholder|insider|registered holder))"
        r"[\s:\n|]+([A-Za-z0-9\s.,'()@&-]+?)\n",
        text_all, re.I | re.MULTILINE,
    )
    if m:
        page_name = m.group(1).strip()
    if not page_name:
        m2 = re.search(
            r"(?:MR|MRS|MS|MADAM|DATO|DATUK|DR|TAN SRI|PUAN|ENCIK)[\s\n]+([A-Za-z\s.,'@-]+?)"
            r"(?:\n\n|\n\s*\n|\n\s*(?:Director|Principal Officer|Major|Others|Group|CEO|CFO|COO|Secretary))",
            text_all, re.I,
        )
        if m2:
            page_name = m2.group(1).strip()

    all_records = []

    for table in soup.find_all("table"):
        table_recs = []
        rows = table.find_all("tr")
        if not rows:
            continue

        headers = [h.get_text(strip=True) for h in rows[0].find_all(["th", "td"])]

        # Horizontal (multi-column) layout
        if len(headers) > 2 and any(_match(h, DATE_KW) for h in headers):
            for data_row in rows[1:]:
                rec = _new_rec()
                vals = [td.get_text(strip=True) for td in data_row.find_all("td")]
                for i, h in enumerate(headers):
                    if i >= len(vals):
                        break
                    v = vals[i]
                    if _match(h, NAME_KW):               rec["Name"] = v
                    elif _match(h, DESIG_KW):            rec["Designation"] = v
                    elif _match(h, DESC_KW):             rec["Description of Securities"] = v
                    elif _match(h, DATE_KW):             rec["Date of Transaction"] = v
                    elif _match(h, SHARES_KW):           rec["No. of Shares"] = _clean_num(v)
                    elif _match(h, PRICE_KW):
                        mp = re.search(r"([\d,]+\.?\d*)", v)
                        if mp: rec["Price (RM)"] = _clean_num(mp.group(1))
                    elif _match(h, TYPE_KW):             rec["Transaction Type"] = v
                if rec["Date of Transaction"] or rec["No. of Shares"]:
                    table_recs.append(rec)
        else:
            # Vertical (label-value) layout — may have multiple insider blocks
            cur_rec = None
            for tr in rows:
                cells = tr.find_all(["td", "th"])
                vals = [c.get_text(strip=True) for c in cells]
                if not vals:
                    continue
                if len(vals) >= 2 and vals[0].lower() == "no" and vals[1].lower() == "salutation":
                    continue

                # New insider record starts with salutation row
                if len(vals) >= 3 and any(
                    s in vals[1].upper()
                    for s in ["MR", "MRS", "MS", "DATO", "DATUK", "DR", "TAN SRI", "PUAN", "ENCIK"]
                ):
                    if cur_rec and (cur_rec["Date of Transaction"] or cur_rec["No. of Shares"]):
                        table_recs.append(cur_rec)
                    cur_rec = _new_rec()
                    cur_rec["Name"] = vals[2]
                    if len(vals) >= 4:
                        d = vals[3]
                        if len(vals) >= 5 and d.lower() == "others":
                            d = vals[4]
                        cur_rec["Designation"] = d
                else:
                    if len(vals) < 2:
                        continue
                    label, val = vals[0], vals[1]
                    if not cur_rec:
                        cur_rec = _new_rec()

                    if _match(label, NAME_KW) and not cur_rec["Name"]:
                        if val: cur_rec["Name"] = val
                    elif _match(label, DESIG_KW) and not cur_rec["Designation"]:
                        if val: cur_rec["Designation"] = val
                    elif _match(label, DESC_KW) and not cur_rec["Description of Securities"]:
                        if val: cur_rec["Description of Securities"] = val
                    elif _match(label, DATE_KW):
                        if val: cur_rec["Date of Transaction"] = val
                    elif _match(label, SHARES_KW):
                        if val: cur_rec["No. of Shares"] = _clean_num(val)
                    elif _match(label, PRICE_KW):
                        mp = re.search(r"([\d,]+\.?\d*)", val)
                        if mp: cur_rec["Price (RM)"] = _clean_num(mp.group(1))
                    # High-priority aggressive sniff for deal types hiding in any label/value
                    vl_lab = label.lower()
                    vl_val = val.lower()
                    if any(w in vl_lab or w in vl_val for w in ["acquired", "acquisition", "bought", "purchase"]):
                        cur_rec["Transaction Type"] = "Acquired"
                    elif any(w in vl_lab or w in vl_val for w in ["disposed", "disposal", "sold", "sale"]):
                        cur_rec["Transaction Type"] = "Disposed"
                    elif _match(label, TYPE_KW) and not cur_rec["Transaction Type"]:
                        if val: cur_rec["Transaction Type"] = val
                    elif "description of \"others\" designation" in label.lower() and val:
                        if not cur_rec["Designation"] or cur_rec["Designation"].lower() == "others":
                            cur_rec["Designation"] = val

            if cur_rec and (cur_rec["Date of Transaction"] or cur_rec["No. of Shares"]):
                table_recs.append(cur_rec)

        for r in table_recs:
            if not r["Name"]:
                r["Name"] = page_name
        all_records.extend(table_recs)

    # Text fallback
    if not all_records:
        rec = _new_rec()
        rec["Name"] = page_name
        m_dt = re.search(
            r"(?:date of (?:transaction|dealing|change))[\s:\n]+([\d]{1,2}\s+[A-Za-z]+\s+[\d]{4}|\d{1,2}/\d{1,2}/\d{4})",
            text_all, re.I,
        )
        if m_dt: rec["Date of Transaction"] = m_dt.group(1).strip()
        m_sh = re.search(r"(?:number of shares|[Nn]o\.? of securities|quantity)[\s:\n]+([\d,]+)", text_all, re.I)
        if m_sh: rec["No. of Shares"] = _clean_num(m_sh.group(1))
        m_pr = re.search(r"(?:price|consideration)[\sA-Za-z/()]*?[:\r\n\s]+(?:MYR|RM)?\s*([\d,]+\.\d+)", text_all, re.I)
        if m_pr: rec["Price (RM)"] = _clean_num(m_pr.group(1))
        m_ty = re.search(r"(?:nature|type) of (?:transaction|interest)[\s:\n]+([A-Za-z\s]+?)\n", text_all, re.I)
        if m_ty: rec["Transaction Type"] = m_ty.group(1).strip()
        m_ds = re.search(r"(?:description|class|type) of securities[\s:\n]+([A-Za-z\s]+?)\n", text_all, re.I)
        if m_ds: rec["Description of Securities"] = m_ds.group(1).strip()
        if any(v for k, v in rec.items() if k != "Name"):
            all_records.append(rec)

    return all_records


# ──────────────────────────────────────────────────────────────────────────────
# STEP 1 — Collect announcement links from JSON API
# ──────────────────────────────────────────────────────────────────────────────
def _collect_links(session, company_code: str, category: str, pages: int):
    """Returns (links, api_stats) where api_stats has pages_fetched, total_rows_seen, sample_titles."""
    main_url = MAIN_URL.format(company=company_code)
    api_headers = {**HEADERS, "Referer": main_url}
    api_stats = {"pages_fetched": 0, "total_rows_seen": 0, "sample_titles": []}

    # Warm up the session with a visit to the main page
    try:
        # Visit the base URL first to set initial cookies
        session.get(BASE, timeout=15)
        # Then visit the announcements page
        session.get(main_url, headers={"Referer": BASE}, timeout=15)
        log.info("Cloudflare clearance / Session warmup OK")
    except Exception as e:
        log.warning(f"Clearance request failed (continuing anyway): {e}")


    links = []
    for p in range(1, pages + 1):
        url = API_URL.format(company=company_code, category=category, page=p)
        # Optional: Add retry logic for API calls
        max_retries = 2
        r = None
        for attempt in range(max_retries + 1):

            try:
                r = session.get(url, headers=api_headers, timeout=15)
                if r.status_code == 200:
                    break
                elif r.status_code in [403, 429]:
                    log.warning(f"API page {p} attempt {attempt+1}: HTTP {r.status_code}. Possible block.")
                    if attempt < max_retries:
                        time.sleep(random.uniform(2, 5))
                        continue
            except Exception as e:
                log.warning(f"API page {p} request error attempt {attempt+1}: {e}")
                if attempt < max_retries:
                    time.sleep(1)
                    continue
                break
        if r and r.status_code == 403:
            log.warning(f"API page {p} returned 403. Attempting HTML fallback...")
            fallback_links = _scrape_links_from_html(session, company_code, category, p)
            if fallback_links:
                links.extend(fallback_links)
                api_stats["pages_fetched"] += 1
                api_stats["total_rows_seen"] += len(fallback_links)
                continue
            else:
                log.error("HTML Fallback failed to find any data.")
                snippet = r.text[:500].replace('\n', ' ')
                api_stats["errors"].append(f"API 403 & Fallback Failed. Snippet: {snippet}")
                break

        if not r or r.status_code != 200:
            log.warning(f"API page {p}: HTTP {getattr(r, 'status_code', 'No Response')}")
            break

        try:
            data = r.json()
            rows = data.get("data", [])
        except Exception as e:
            snippet = r.text[:500].replace('\n', ' ')
            log.warning(f"API page {p} JSON parse error (is it a challenge page?): {snippet}")
            api_stats["errors"].append(f"Page {p} JSON error: {snippet}")
            break


        api_stats["pages_fetched"] += 1
        api_stats["total_rows_seen"] += len(rows)
        log.info(f"Page {p}: {len(rows)} items")
        if not rows:
            break

        for row in rows:
            if len(row) <= 3:
                continue
            title_soup = BeautifulSoup(row[3], "html.parser")
            a = title_soup.find("a", href=True)
            if not a:
                continue

            raw_title = a.get_text(strip=True)
            title = raw_title.upper()

            # Capture sample titles for diagnostics (first 5 unique)
            if len(api_stats["sample_titles"]) < 5 and raw_title not in api_stats["sample_titles"]:
                api_stats["sample_titles"].append(raw_title)

            # DRCO category is extremely clean for insider actions.
            # We just exclude notices of 'intention' to deal, which aren't executed trades yet.
            is_dealings = "INTENTION" not in title

            if not is_dealings:
                continue

            href = a["href"]
            full_url = href if href.startswith("http") else f"{BASE}{href}"

            date_soup = BeautifulSoup(row[1], "html.parser")
            date_text = " ".join(date_soup.stripped_strings)
            date_posted = " ".join(date_text.split()[:3])

            links.append({"href": full_url, "title": raw_title, "date_posted": date_posted})

        # Small polite delay between API pages only
        time.sleep(0.3)

    if not links and not api_stats.get("errors"):
        # If no links and no errors, but we visited pages, maybe it's just empty.
        # But if total_rows_seen is 0 and pages_fetched > 0, it's suspicious.
        if api_stats["pages_fetched"] > 0 and api_stats["total_rows_seen"] == 0:
             api_stats["errors"].append("API returned 0 rows despite successful requests. This often happens when Cloudflare silently filters data center IPs.")
    
    log.info(f"Total qualifying links: {len(links)}")
    return links, api_stats


def _scrape_links_from_html(session, company_code: str, category: str, page: int):

    """Fallback: Scrapes the static HTML table if the API is blocked."""
    url = MAIN_URL.format(company=company_code)
    # The HTML page might only show the first page easily, but it's better than nothing.
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200:
            return []
        
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.find("table", {"id": "announcementsTable"})
        if not table:
            # Try a broader search
            table = soup.find("table")
        
        if not table:
            return []
            
        rows = table.find_all("tr")[1:] # skip header
        html_links = []
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            
            # Date
            date_posted = cells[1].get_text(strip=True)
            # Title & Link
            a = cells[3].find("a", href=True)
            if not a:
                continue
            
            title = a.get_text(strip=True)
            if "INTENTION" in title.upper():
                continue
                
            href = a["href"]
            full_url = href if href.startswith("http") else f"{BASE}{href}"
            
            html_links.append({"href": full_url, "title": title, "date_posted": date_posted})
            
        return html_links
    except Exception as e:
        log.warning(f"HTML fallback error: {e}")
        return []

# ──────────────────────────────────────────────────────────────────────────────
# STEP 2 — Fetch one detail page + iframe (called in parallel)
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_detail_page(session, referer: str, lnk: dict, logger) -> list:
    """Returns list of raw result dicts for one announcement link."""
    href  = lnk["href"]
    title = lnk["title"]
    results = []

    try:
        headers = {"Referer": referer}
        r1 = session.get(href, headers=headers, timeout=15)
        if r1.status_code != 200:
            logger.warning(f"Detail page fetch failed (HTTP {r1.status_code}): {href}")
            return results

        soup = BeautifulSoup(r1.text, "lxml")
        
        iframe = soup.find("iframe", id="bm_ann_detail_iframe")
        if not iframe or not iframe.get("src"):
            # Some old pages don't have iframes, they have the content directly
            # Or maybe it's a different ID.
            logger.debug(f"No iframe found for {href}")
            entries = parse_detail(r1.text)
            if not entries:
                return results
        else:
            src = iframe["src"]
            frame_url = src if src.startswith("http") else f"{BASE}{src}"
            
            r2 = session.get(frame_url, headers=headers, timeout=15)
            if r2.status_code != 200:
                logger.warning(f"Iframe fetch failed (HTTP {r2.status_code}): {frame_url}")
                return results
            entries = parse_detail(r2.text)

        for entry in entries:
            results.append({
                "Date Posted":          lnk["date_posted"],
                "Title":                title,
                "Name":                 entry["Name"],
                "Designation":          entry["Designation"],
                "Description":          entry["Description of Securities"],
                "Date of Transaction":  entry["Date of Transaction"],
                "Price (RM)":           entry["Price (RM)"],
                "No. of Shares":        entry["No. of Shares"],
                "Transaction Type":     entry["Transaction Type"],
                "URL":                  href,
            })
        return results
    except Exception as e:
        logger.warning(f"Detail fetch exception for {href}: {e}")
        return results



# ──────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION
# ──────────────────────────────────────────────────────────────────────────────
def check_connection(proxy=None):
    """Debug helper to check IP and Bursa connectivity from the current environment."""
    results = {"ip": "Unknown", "bursa_status": "Unknown", "bursa_snippet": "", "error": None}
    proxies = {"http": proxy, "https": proxy} if proxy else None
    
    try:
        # 1. Check Public IP (using a stable service)
        session = cffi_requests.Session(impersonate=IMPERSONATE, proxies=proxies)
        r_ip = session.get("https://httpbin.org/ip", timeout=10)
        if r_ip.status_code == 200:
            results["ip"] = r_ip.json().get("origin", "Unknown")
            
        # 2. Check Bursa Malaysia
        # We try the same warmup logic as the main scraper
        r_bursa = session.get(BASE, timeout=10)
        results["bursa_status"] = r_bursa.status_code
        if r_bursa.status_code != 200:
            results["bursa_snippet"] = r_bursa.text[:500].replace('\n', ' ')
        else:
            results["bursa_snippet"] = "Success! Homepage is accessible."
            
    except Exception as e:
        results["error"] = str(e)
        
    return results


def scrape(company_code: str = COMPANY_CODE, category: str = CATEGORY_ID, pages: int = PAGES_TO_SCRAPE, proxy: str = None):

    """Returns (DataFrame, stats_dict). proxy should be a URL like 'http://user:pass@ip:port'"""
    stats = {"pages_fetched": 0, "links_found": 0, "raw_results": 0, "after_filter": 0, "errors": [], "sample_titles": []}


    log.info(f"Bursa Fast Scraper — Company: {company_code}  Category: {category}  Pages: {pages}")
    main_url = MAIN_URL.format(company=company_code)

    # On Linux/Streamlit, Chrome profiles are most reliable.
    profiles = ["chrome110", "chrome101", "chrome120", IMPERSONATE]



    
    session = None
    for profile in profiles:
        try:
            proxies = {"http": proxy, "https": proxy} if proxy else None
            session = cffi_requests.Session(impersonate=profile, proxies=proxies)
            log.info(f"Attempting with profile: {profile}")
            
            # Warmup
            session.get(BASE, timeout=15)
            r_warm = session.get(main_url, headers={"Referer": BASE}, timeout=15)
            
            if r_warm.status_code == 200:
                log.info(f"Profile {profile} confirmed working.")
                break
            elif r_warm.status_code == 403:
                log.warning(f"Profile {profile} got 403. Trying next...")
                continue
        except Exception as e:
            log.warning(f"Error initializing profile {profile}: {e}")
            continue
    
    if not session:
        # Final fallback
        session = cffi_requests.Session(impersonate="chrome110", proxies=proxies)
        log.warning("All profiles failed. Using fallback chrome110.")

    # --- Step 1: collect links via API ---
    links, api_stats = _collect_links(session, company_code, category, pages)

    stats["pages_fetched"] = api_stats["pages_fetched"]
    stats["total_rows_seen"] = api_stats["total_rows_seen"]
    stats["sample_titles"] = api_stats["sample_titles"]
    stats["links_found"] = len(links)
    if api_stats.get("errors"):
        stats["errors"].extend(api_stats["errors"])


    if not links:
        log.warning("No qualifying announcement links found.")
        return pd.DataFrame(), stats

    # --- Step 2: fetch detail pages in parallel ---
    log.info(f"Fetching {len(links)} detail pages with {MAX_WORKERS} parallel workers…")
    raw_results = []
    main_url_for_ref = MAIN_URL.format(company=company_code)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_lnk = {
            executor.submit(_fetch_detail_page, session, main_url_for_ref, lnk, log): lnk 
            for lnk in links
        }
        for future in as_completed(future_to_lnk):
            try:
                raw_results.extend(future.result())
            except Exception as e:
                lnk = future_to_lnk[future]
                log.warning(f"Worker error for {lnk['href']}: {e}")
                stats["errors"].append(str(e))

    stats["raw_results"] = len(raw_results)
    log.info(f"Raw results before filter: {len(raw_results)}")

    # --- Step 3: filter strictly for Acquired / Disposed ---
    results = []
    for r in raw_results:
        t_type = str(r.get("Transaction Type") or "").lower()
        d_sec  = str(r.get("Description") or "").lower()

        # 1. Primary classification mapping
        if any(w in t_type for w in ["acqui", "bought", "purchase"]):
            r["Transaction Type"] = "Acquired"
        elif any(w in t_type for w in ["dispos", "sold", "sale"]):
            r["Transaction Type"] = "Disposed"
        elif category == "DRCO":
             # Try deeper sniff on description if type is missing
             if any(w in d_sec for w in ["acqui", "bought", "purchase"]):
                 r["Transaction Type"] = "Acquired"
             elif any(w in d_sec for w in ["dispos", "sold", "sale"]):
                 r["Transaction Type"] = "Disposed"

        # 2. Final strict filter: ONLY Acquired or Disposed allowed
        if r.get("Transaction Type") in ["Acquired", "Disposed"]:
             # Basic filter for ordinary shares
             is_ordinary = "ordinary share" in d_sec or d_sec.strip() == ""
             if is_ordinary:
                 # --- Auto-correction for total consideration ---
                 # If Price > 1000, it's likely total RM amount, not price per share.
                 price = r.get("Price (RM)")
                 shares = r.get("No. of Shares")
                 if price and shares and price > 1000 and shares > 0:
                     r["Price (RM)"] = round(price / shares, 4)
                 
                 results.append(r)

    df = pd.DataFrame(results)
    stats["after_filter"] = len(df)



    if not df.empty:
        for col in ("Date Posted", "Date of Transaction"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
        df.sort_values("Date Posted", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

    log.info(f"Done. Final rows: {len(df)}")
    return df, stats


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Bursa Malaysia Announcements")
    parser.add_argument("--company",  default=COMPANY_CODE)
    parser.add_argument("--category", default=CATEGORY_ID)
    parser.add_argument("--pages",    type=int, default=PAGES_TO_SCRAPE)
    args = parser.parse_args()

    df, stats = scrape(company_code=args.company, category=args.category, pages=args.pages)
    print(f"\nStats: {stats}")

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", 40)
    pd.set_option("display.width", 160)

    if not df.empty:
        print(df[["Date of Transaction", "Name", "Designation", "Price (RM)", "No. of Shares", "Transaction Type"]].to_string(index=False))
        output_file = f"{args.company}_bursa_dealings.csv"
        df.to_csv(output_file, index=False)
        print(f"\nSaved {len(df)} rows to {output_file}")
    else:
        print("No data extracted.")
