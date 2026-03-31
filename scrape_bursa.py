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

IMPERSONATE = "chrome120"

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
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
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
             "price (rm", "price(rm", "market price", "consideration")
TYPE_KW   = ("nature of transaction", "type of transaction", "transaction type", "nature of dealing", "circumstances", "nature of change")
DESIG_KW  = ("designation", "position", "title")
DESC_KW   = ("description of securities", "class of securities", "type of securities",
             "description of security")


def _clean_num(v):
    if v is None:
        return None
    n = re.sub(r"[^\d.]", "", str(v))
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
        session.get(main_url, timeout=15)
        log.info("Cloudflare clearance OK")
    except Exception as e:
        log.warning(f"Clearance request failed (continuing anyway): {e}")

    links = []
    for p in range(1, pages + 1):
        url = API_URL.format(company=company_code, category=category, page=p)
        try:
            r = session.get(url, headers=api_headers, timeout=15)
        except Exception as e:
            log.warning(f"API page {p} request error: {e}")
            break

        if r.status_code != 200:
            log.warning(f"API page {p}: HTTP {r.status_code}")
            break

        try:
            data = r.json()
            rows = data.get("data", [])
        except Exception as e:
            log.warning(f"API page {p} JSON parse error: {e}")
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

    log.info(f"Total qualifying links: {len(links)}")
    return links, api_stats


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
        soup = BeautifulSoup(r1.text, "lxml")
        
        iframe = soup.find("iframe", id="bm_ann_detail_iframe")
        if not iframe or not iframe.get("src"):
            return results

        src = iframe["src"]
        frame_url = src if src.startswith("http") else f"{BASE}{src}"
        
        r2 = session.get(frame_url, headers=headers, timeout=15)
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
        logger.warning(f"Detail fetch failed for {href}: {e}")
        return results


# ──────────────────────────────────────────────────────────────────────────────
# MAIN PUBLIC FUNCTION
# ──────────────────────────────────────────────────────────────────────────────
def scrape(company_code: str = COMPANY_CODE, category: str = CATEGORY_ID, pages: int = PAGES_TO_SCRAPE, proxy: str = None):
    """Returns (DataFrame, stats_dict). proxy should be a URL like 'http://user:pass@ip:port'"""
    stats = {"pages_fetched": 0, "links_found": 0, "raw_results": 0, "after_filter": 0, "errors": []}

    log.info(f"Bursa Fast Scraper — Company: {company_code}  Category: {category}  Pages: {pages}")

    # Create a session with a strict modern chrome profile + proxy if provided
    proxies = {"http": proxy, "https": proxy} if proxy else None
    
    try:
        session = cffi_requests.Session(impersonate=IMPERSONATE, proxies=proxies)
        log.info(f"Created standard session with profile: {IMPERSONATE}")
    except Exception as e:
        log.warning(f"Failed to create session with profile {IMPERSONATE}: {e}. Falling back to default.")
        session = cffi_requests.Session(impersonate="chrome110", proxies=proxies)

    # --- Step 1: collect links via API ---
    links, api_stats = _collect_links(session, company_code, category, pages)
    stats["pages_fetched"] = api_stats["pages_fetched"]
    stats["total_rows_seen"] = api_stats["total_rows_seen"]
    stats["sample_titles"] = api_stats["sample_titles"]
    stats["links_found"] = len(links)

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

    # --- Step 3: filter ---
    results = []
    for r in raw_results:
        t_type = str(r.get("Transaction Type") or "").lower()
        d_sec  = str(r.get("Description") or "").lower()

        is_deal    = any(t in t_type for t in ["acquired", "acquisition", "disposed", "disposal", "bought", "sold", "interest"])
        
        # We explicitly pass DRCO category rows because they are inherently insider dealing updates.
        is_drco    = (category == "DRCO")
        
        # Pass if description has "ordinary share" OR is empty (often unparsed)
        is_ordinary = "ordinary share" in d_sec or d_sec.strip() == ""

        if (is_deal or is_drco) and is_ordinary:
            # Coerce the transaction type so the Streamlit frontend can plot it
            if is_drco and not is_deal:
                if "acqui" in d_sec or "bought" in d_sec:
                    r["Transaction Type"] = "Acquired"
                elif "dispos" in d_sec or "sold" in d_sec:
                    r["Transaction Type"] = "Disposed"

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
