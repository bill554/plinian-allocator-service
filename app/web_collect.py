"""
Web text collection for allocator research.

Scrapes allocator websites to extract relevant text from:
- About/Overview pages
- Investment/Portfolio pages  
- Policy documents and RFPs
- Annual reports, CAFRs, and board materials

Supports both HTML and PDF extraction with intelligent page targeting.
"""

import re
import logging
from io import BytesIO
from urllib.parse import urljoin

import httpx
import trafilatura

logger = logging.getLogger(__name__)

# Try pdfplumber first (better for tables), fall back to pypdf
try:
    import pdfplumber
    PDF_EXTRACTOR = "pdfplumber"
except ImportError:
    pdfplumber = None
    PDF_EXTRACTOR = "pypdf"

from pypdf import PdfReader


DEFAULT_TIMEOUT = 20  # increased for large PDFs
MAX_TEXT_CHARS = 25000  # per bucket for about/policy
MAX_REPORT_TEXT_CHARS = 50000  # larger limit for report_text (PDF content)
MAX_URLS_PER_BUCKET = 10
MAX_PDF_SIZE_MB = 50  # skip PDFs larger than this


# ----- 1. Helper: safe HTTP fetch ----- #

def safe_get(url: str) -> tuple:
    """
    Fetch a URL and return (content_type, raw_bytes or text).
    Returns ("", "") on failure.
    """
    if not url:
        return "", ""

    try:
        with httpx.Client(follow_redirects=True, timeout=DEFAULT_TIMEOUT) as client:
            # First do a HEAD request to check size for PDFs
            if url.lower().endswith(".pdf"):
                try:
                    head = client.head(url)
                    content_length = int(head.headers.get("content-length", 0))
                    if content_length > MAX_PDF_SIZE_MB * 1024 * 1024:
                        logger.warning(f"PDF too large ({content_length / 1024 / 1024:.1f}MB): {url}")
                        return "", ""
                except Exception:
                    pass  # Continue anyway if HEAD fails
            
            resp = client.get(url)
        if resp.status_code != 200:
            return "", ""

        content_type = resp.headers.get("content-type", "").lower()
        is_pdf = "pdf" in content_type or url.lower().endswith(".pdf")
        return content_type, resp.content if is_pdf else resp.text
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return "", ""


# ----- 2. Helper: extract text from HTML ----- #

def extract_text_from_html(html: str) -> str:
    """Extract clean text from HTML using trafilatura."""
    if not html:
        return ""
    try:
        extracted = trafilatura.extract(html, include_comments=False)
        return extracted or ""
    except Exception:
        return ""


# ----- 3. CAFR/PDF Section Detection ----- #

# Keywords that indicate high-value pages in CAFRs/annual reports
INVESTMENT_SECTION_MARKERS = [
    "investment section",
    "report from the chief investment officer",
    "chief investment officer",
    "report of the cio",
    "investment report",
    "investment overview",
    "asset allocation",
    "investment policy",
    "investment consultant",
    "investment performance",
]

HIGH_VALUE_KEYWORDS = [
    "asset allocation", "asset class", "target allocation", "actual allocation",
    "private equity", "private markets", "real estate", "real assets",
    "hedge fund", "absolute return", "fixed income", "public equity",
    "investment policy", "investment strategy", "investment philosophy",
    "chief investment officer", "cio", "investment staff", "investment team",
    "consultant", "verus", "nepc", "callan", "mercer", "cambridge",
    "commitment", "committed", "co-invest", "coinvest", "direct investment",
    "manager", "fund commitment", "private credit", "infrastructure",
    "emerging manager", "diverse manager", "risk parity", "commodities",
    "performance", "benchmark", "return", "allocation percentage",
    "board of trustees", "executive director", "fiduciary"
]


def _score_page_relevance(text: str) -> int:
    """Score a page's relevance based on investment keywords."""
    if not text:
        return 0
    text_lower = text.lower()
    score = 0
    
    # High score for investment section markers
    for marker in INVESTMENT_SECTION_MARKERS:
        if marker in text_lower:
            score += 5
    
    # Regular score for other keywords
    for kw in HIGH_VALUE_KEYWORDS:
        if kw in text_lower:
            score += 1
    
    return score


def _find_investment_section_pages(pdf, total_pages: int) -> tuple[int, int]:
    """
    Scan PDF to find the Investment Section page range.
    Returns (start_page, end_page) or (None, None) if not found.
    
    CAFRs typically have a Table of Contents in first 5-10 pages that lists:
    - Introductory Section
    - Financial Section  
    - Investment Section (THIS IS WHAT WE WANT)
    - Actuarial Section
    - Statistical Section
    """
    investment_start = None
    investment_end = None
    
    # First, scan early pages for TOC to find Investment Section page number
    toc_pages = min(15, total_pages)
    
    for i in range(toc_pages):
        try:
            if hasattr(pdf, 'pages'):  # pdfplumber
                text = pdf.pages[i].extract_text() or ""
            else:  # pypdf
                text = pdf.pages[i].extract_text() or ""
            
            text_lower = text.lower()
            
            # Look for TOC entry like "Investment Section...45" or "Investment Section 45"
            # Common patterns in CAFRs
            import re
            
            # Pattern: "investment section" followed by page number
            patterns = [
                r'investment\s+section[.\s]*(\d+)',
                r'report.*chief investment officer[.\s]*(\d+)',
                r'cio\s+report[.\s]*(\d+)',
                r'investment\s+overview[.\s]*(\d+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text_lower)
                if match:
                    try:
                        page_num = int(match.group(1))
                        # PDF pages are often offset by cover pages
                        # Try the exact number and nearby pages
                        investment_start = max(0, page_num - 3)
                        logger.info(f"Found Investment Section reference at TOC, targeting page ~{page_num}")
                        break
                    except:
                        pass
            
            if investment_start:
                break
                
        except Exception as e:
            logger.debug(f"Error scanning page {i} for TOC: {e}")
            continue
    
    # If we found a start from TOC, estimate the section is ~30-40 pages
    if investment_start:
        investment_end = min(investment_start + 40, total_pages)
        return investment_start, investment_end
    
    # Fallback: scan for section header directly
    # Investment Section usually starts around page 40-80 in most CAFRs
    scan_start = min(30, total_pages)
    scan_end = min(120, total_pages)
    
    for i in range(scan_start, scan_end):
        try:
            if hasattr(pdf, 'pages'):
                text = pdf.pages[i].extract_text() or ""
            else:
                text = pdf.pages[i].extract_text() or ""
            
            text_lower = text.lower()
            
            # Look for section header
            if any(marker in text_lower for marker in [
                "investment section",
                "report from the chief investment officer", 
                "report of the chief investment officer",
                "chief investment officer's report"
            ]):
                investment_start = i
                investment_end = min(i + 40, total_pages)
                logger.info(f"Found Investment Section header at page {i}")
                return investment_start, investment_end
                
        except Exception as e:
            continue
    
    logger.info("Could not locate Investment Section, will use smart sampling")
    return None, None


def extract_text_from_pdf_pdfplumber(data: bytes, max_pages: int = 100) -> str:
    """
    Extract text from PDF using pdfplumber (better for tables).
    Specifically targets CAFR structure:
    1. First 15 pages (intro, exec summary, board/staff list)
    2. Investment Section (found via TOC or header scan)
    3. Any other high-value pages
    """
    if not data or not pdfplumber:
        return ""
    
    try:
        pdf = pdfplumber.open(BytesIO(data))
    except Exception as e:
        logger.debug(f"pdfplumber failed to open PDF: {e}")
        return ""
    
    total_pages = len(pdf.pages)
    logger.info(f"PDF has {total_pages} pages, using pdfplumber with CAFR-aware extraction")
    
    pages_to_read = set()
    
    # ALWAYS read first 15 pages (intro, letter from ED, board list, staff, TOC)
    for i in range(min(15, total_pages)):
        pages_to_read.add(i)
    
    # Find Investment Section
    inv_start, inv_end = _find_investment_section_pages(pdf, total_pages)
    
    if inv_start is not None:
        # Read the entire Investment Section
        for i in range(inv_start, inv_end):
            pages_to_read.add(i)
        logger.info(f"Will read Investment Section pages {inv_start}-{inv_end}")
    else:
        # Fallback: sample middle pages where investment content usually lives
        sample_ranges = [
            (15, 50, 3),    # Pages 15-50, every 3rd page
            (50, 100, 2),   # Pages 50-100, every 2nd page (investment section often here)
            (100, 150, 4),  # Pages 100-150, every 4th page
        ]
        
        for start, end, step in sample_ranges:
            if start >= total_pages:
                break
            actual_end = min(end, total_pages)
            for i in range(start, actual_end, step):
                pages_to_read.add(i)
    
    # Also read last 5 pages (sometimes has consultant/advisor info)
    for i in range(max(0, total_pages - 5), total_pages):
        pages_to_read.add(i)
    
    # Sort and limit
    pages_to_read = sorted(pages_to_read)[:max_pages]
    logger.info(f"Reading {len(pages_to_read)} pages from PDF")
    
    # Extract text, prioritizing high-value pages
    high_value_texts = []
    regular_texts = []
    
    for i in pages_to_read:
        try:
            page = pdf.pages[i]
            text = page.extract_text() or ""
            
            # Also try to extract tables (common in investment sections)
            try:
                tables = page.extract_tables()
                for table in tables:
                    if table:
                        for row in table:
                            if row:
                                row_text = " | ".join(str(cell) if cell else "" for cell in row)
                                if row_text.strip():
                                    text += "\n" + row_text
            except:
                pass
            
            if text.strip():
                score = _score_page_relevance(text)
                if score >= 3:
                    high_value_texts.append(f"[Page {i+1}]\n{text}")
                else:
                    regular_texts.append(text)
                    
        except Exception as e:
            logger.debug(f"Failed to extract page {i}: {e}")
            continue
    
    pdf.close()
    
    # Prioritize high-value pages at the front
    all_texts = high_value_texts + regular_texts
    result = "\n\n".join(all_texts)
    
    logger.info(f"Extracted {len(result)} chars from {len(pages_to_read)} pages ({len(high_value_texts)} high-value pages)")
    
    return result


def extract_text_from_pdf_pypdf(data: bytes, max_pages: int = 60) -> str:
    """Extract text from PDF bytes using pypdf (fallback)."""
    if not data:
        return ""
    try:
        reader = PdfReader(BytesIO(data))
    except Exception as e:
        logger.debug(f"pypdf failed to open PDF: {e}")
        return ""

    total_pages = len(reader.pages)
    logger.info(f"PDF has {total_pages} pages, using pypdf")
    
    texts = []
    pages_to_read = []
    
    if total_pages <= max_pages:
        pages_to_read = list(range(total_pages))
    else:
        # Smart page selection for large PDFs
        # First 15 pages
        pages_to_read.extend(range(min(15, total_pages)))
        # Middle section (investment content)
        mid_start = max(15, total_pages // 4)
        mid_end = min(total_pages, 3 * total_pages // 4)
        step = max(1, (mid_end - mid_start) // 30)
        for i in range(mid_start, mid_end, step):
            pages_to_read.append(i)
        # Last 10 pages
        for i in range(max(0, total_pages - 10), total_pages):
            pages_to_read.append(i)
        
        pages_to_read = sorted(set(pages_to_read))[:max_pages]

    for i in pages_to_read:
        try:
            page = reader.pages[i]
            text = page.extract_text() or ""
            if text.strip():
                texts.append(text)
        except Exception:
            continue

    return "\n\n".join(texts)


def extract_text_from_pdf(data: bytes) -> str:
    """Extract text from PDF bytes using best available library."""
    if not data:
        return ""
    
    # Try pdfplumber first (better for tables)
    if pdfplumber:
        result = extract_text_from_pdf_pdfplumber(data)
        if result:
            return result
    
    # Fall back to pypdf
    return extract_text_from_pdf_pypdf(data)


def extract_text(url: str) -> str:
    """Fetch URL and extract text (handles both HTML and PDF)."""
    content_type, body = safe_get(url)
    if not content_type or body == "":
        return ""

    if "pdf" in content_type or url.lower().endswith(".pdf"):
        # PDF
        if isinstance(body, str):
            body = body.encode("utf-8", errors="ignore")
        logger.info(f"Extracting PDF: {url}")
        return extract_text_from_pdf(body)
    else:
        # Assume HTML or text
        if isinstance(body, bytes):
            try:
                body = body.decode("utf-8", errors="ignore")
            except Exception:
                return ""
        return extract_text_from_html(body)


def find_pdf_links_in_html(html: str, base_url: str) -> list:
    """
    Extract PDF links from HTML page.
    Returns list of absolute PDF URLs.
    """
    if not html:
        return []
    
    pdf_urls = []
    
    # Find all href attributes pointing to PDFs
    href_pattern = r'href=["\']([^"\']*\.pdf)["\']'
    matches = re.findall(href_pattern, html, re.IGNORECASE)
    
    for match in matches:
        # Convert to absolute URL
        if match.startswith("http"):
            pdf_urls.append(match)
        elif match.startswith("/"):
            pdf_urls.append(urljoin(base_url, match))
        else:
            pdf_urls.append(urljoin(base_url + "/", match))
    
    # Also look for common patterns in text
    # e.g., "Download Annual Report (PDF)"
    return list(set(pdf_urls))


def fetch_page_and_find_pdfs(url: str) -> tuple:
    """
    Fetch a page and return both its text and any PDF links found.
    Returns (page_text, [pdf_urls])
    """
    content_type, body = safe_get(url)
    if not content_type or body == "":
        return "", []
    
    if "pdf" in content_type or url.lower().endswith(".pdf"):
        # It's already a PDF
        if isinstance(body, str):
            body = body.encode("utf-8", errors="ignore")
        return extract_text_from_pdf(body), []
    
    # It's HTML - extract text and find PDF links
    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8", errors="ignore")
        except Exception:
            return "", []
    
    page_text = extract_text_from_html(body)
    pdf_links = find_pdf_links_in_html(body, url)
    
    return page_text, pdf_links


# ----- 3. Path sets for allocators ----- #

ABOUT_PATHS = [
    "/about",
    "/about-us",
    "/who-we-are",
    "/our-story",
    "/company",
    "/overview",
    "/mission",
    "/what-we-do",
    "/organization",
    "/organization-overview",
    "/team",
    "/our-team",
    "/leadership",
    "/management",
    "/executive-team",
    "/board",
    "/board-of-trustees",
    "/board-of-directors",
    "/staff",
    "/people",
    "/investment-team",
    "/investments-team",
    "/investment-office",
    "/investment-committee",
]

INVESTMENT_PATHS = [
    "/investments",
    "/investment",
    "/investment-office",
    "/investment-division",
    "/investment-department",
    "/portfolio",
    "/fund-portfolio",
    "/investment-portfolio",
    "/portfolio-overview",
    "/holdings",
    "/strategy",
    "/investment-strategy",
    "/asset-allocation",
    "/assetallocation",
    "/allocations",
    "/investment-philosophy",
    "/alternatives",
    "/real-assets",
    "/private-equity",
    "/private-markets",
    "/real-estate",
    "/hedge-funds",
    "/credit",
    "/private-credit",
    "/infrastructure",
    "/natural-resources",
    "/direct-investing",
    "/direct-investments",
    "/co-invest",
    "/co-investments",
    "/coinvest",
    "/coinvestments",
]

POLICY_PATHS = [
    "/investment-policy",
    "/ips",
    "/investment-policy-statement",
    "/investment-guidelines",
    "/investment-principles",
    "/investment-committee",
    "/committees",
    "/governance",
    "/oversight",
    "/policies",
    "/rfp",
    "/requests-for-proposals",
    "/vendor-opportunities",
    "/manager-search",
    "/emerging-manager",
    "/emerging-managers",
    "/em-program",
    "/small-manager",
    "/diverse-manager",
]

REPORT_PATHS = [
    "/annual-report",
    "/annualreports",
    "/reports",
    "/financial-reports",
    "/financials",
    "/publications",
    "/cafr",
    "/cafrs",
    "/audit-reports",
    "/actuarial-reports",
    "/monthly-report",
    "/quarterly-report",
    "/investment-reports",
    "/performance-reports",
    "/market-commentary",
    "/meetings",
    "/board-meetings",
    "/committee-meetings",
    "/minutes",
    "/agendas",
]


# ----- 4. Helper: build base URL from Notion page ----- #

def get_base_url_from_notion_page(allocator_page: dict) -> str:
    """
    Try to derive a base URL from the Notion allocator page properties:
    - Prefer 'Main Website' (URL property)
    - Fallback to 'Domain' (Text) and assume https://
    Returns "" if nothing usable.
    """
    props = allocator_page.get("properties", {})

    # Try Main Website first
    main_site_prop = props.get("Main Website", {})
    if main_site_prop.get("type") == "url":
        main_site = main_site_prop.get("url")
        if main_site:
            return main_site.rstrip("/")

    # Try Website
    website_prop = props.get("Website", {})
    if website_prop.get("type") == "url":
        website = website_prop.get("url")
        if website:
            return website.rstrip("/")

    # Fallback to Domain (text field)
    domain = ""
    domain_prop = props.get("Domain", {})
    if domain_prop.get("type") == "rich_text":
        texts = domain_prop.get("rich_text", [])
        if texts:
            domain = texts[0].get("plain_text", "")
    elif isinstance(domain_prop, str):
        domain = domain_prop

    if domain:
        domain = domain.strip()
        if domain.startswith("http://") or domain.startswith("https://"):
            return domain.rstrip("/")
        return f"https://{domain}".rstrip("/")

    return ""


# ----- 5. Helper: normalize & limit URLs ----- #

def normalize_url(base_url: str, path: str) -> str:
    """Build full URL from base and path."""
    if not base_url:
        return ""
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def unique_urls(urls: list) -> list:
    """Deduplicate URLs while preserving order."""
    seen = set()
    out = []
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def trim_text(text: str, limit: int = MAX_TEXT_CHARS) -> str:
    """Collapse whitespace and trim to limit."""
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text[:limit]


# ----- 6. Main entry: collect_web_text ----- #

def collect_web_text(allocator_page: dict, discovered_urls: dict = None) -> dict:
    """
    Given a Notion allocator page, fetch and aggregate text from relevant URLs.
    
    Args:
        allocator_page: Notion page object with allocator properties
        discovered_urls: Optional dict from web search with discovered URLs:
            {investments_url, annual_report_url, about_url, team_url, pdf_urls}
        
    Returns:
        {
          "about_text": "...",
          "policy_text": "...",
          "report_text": "..."
        }
    """
    props = allocator_page.get("properties", {})
    base_url = get_base_url_from_notion_page(allocator_page)
    discovered_urls = discovered_urls or {}

    # Pre-existing URLs from Notion (if specified)
    investments_page_prop = props.get("Investments Page URL", {})
    investments_page = investments_page_prop.get("url") if investments_page_prop.get("type") == "url" else None
    
    latest_report_prop = props.get("Latest Report URL", {})
    latest_report_url = latest_report_prop.get("url") if latest_report_prop.get("type") == "url" else None

    about_urls = []
    policy_urls = []
    report_urls = []
    pdf_urls = []  # Collect PDF URLs separately for priority fetching

    # Priority 1: Explicit Notion-specified URLs
    if investments_page:
        policy_urls.append(investments_page)
    if latest_report_url:
        if latest_report_url.lower().endswith(".pdf"):
            pdf_urls.append(latest_report_url)
        else:
            report_urls.append(latest_report_url)
    
    # Priority 2: Search-discovered URLs
    if discovered_urls.get("investments_url"):
        policy_urls.append(discovered_urls["investments_url"])
    if discovered_urls.get("annual_report_url"):
        url = discovered_urls["annual_report_url"]
        if url.lower().endswith(".pdf"):
            pdf_urls.append(url)
        else:
            report_urls.append(url)
    if discovered_urls.get("about_url"):
        about_urls.append(discovered_urls["about_url"])
    if discovered_urls.get("team_url"):
        about_urls.append(discovered_urls["team_url"])
    
    # Add any PDFs discovered by search
    if discovered_urls.get("pdf_urls"):
        pdf_urls.extend(discovered_urls["pdf_urls"])

    # Priority 3: Root page as a general "about" source
    if base_url:
        about_urls.append(base_url)

    # Priority 4: Path-based URL guessing
    for p in ABOUT_PATHS:
        about_urls.append(normalize_url(base_url, p))

    for p in INVESTMENT_PATHS:
        policy_urls.append(normalize_url(base_url, p))

    for p in POLICY_PATHS:
        policy_urls.append(normalize_url(base_url, p))

    for p in REPORT_PATHS:
        report_urls.append(normalize_url(base_url, p))

    # Deduplicate
    about_urls = unique_urls(about_urls)
    policy_urls = unique_urls(policy_urls)
    report_urls = unique_urls(report_urls)
    pdf_urls = unique_urls(pdf_urls)

    # Fetch & aggregate text
    about_texts = []
    policy_texts = []
    report_texts = []

    # Fetch about pages
    for url in about_urls[:MAX_URLS_PER_BUCKET]:
        txt = extract_text(url)
        if txt:
            about_texts.append(txt)

    # Fetch policy/investment pages - also look for PDF links
    for url in policy_urls[:MAX_URLS_PER_BUCKET]:
        txt, found_pdfs = fetch_page_and_find_pdfs(url)
        if txt:
            policy_texts.append(txt)
        # Add discovered PDFs to our list
        for pdf_url in found_pdfs:
            if pdf_url not in pdf_urls:
                pdf_urls.append(pdf_url)

    # Fetch report pages - also look for PDF links
    for url in report_urls[:MAX_URLS_PER_BUCKET]:
        txt, found_pdfs = fetch_page_and_find_pdfs(url)
        if txt:
            report_texts.append(txt)
        # Add discovered PDFs to our list (prioritize annual reports/CAFRs)
        for pdf_url in found_pdfs:
            pdf_lower = pdf_url.lower()
            # Prioritize annual reports, CAFRs, and investment reports
            if any(kw in pdf_lower for kw in ["annual", "cafr", "investment", "acfr", "report"]):
                if pdf_url not in pdf_urls:
                    pdf_urls.insert(0, pdf_url)  # Add to front
            elif pdf_url not in pdf_urls:
                pdf_urls.append(pdf_url)

    # Now fetch the most relevant PDFs
    # Sort PDFs by relevance (prefer recent annual reports and board books)
    def pdf_priority(url):
        url_lower = url.lower()
        score = 100  # Base score (lower is better)
        
        # Prefer recent years (FY24, FY25, 2024, 2025)
        if "fy25" in url_lower or "fy24" in url_lower or "2025" in url_lower or "2024" in url_lower:
            score -= 50
        elif "fy23" in url_lower or "2023" in url_lower:
            score -= 40
        elif "fy22" in url_lower or "2022" in url_lower:
            score -= 30
        
        # Board books are VERY valuable - current commitments, manager changes
        if "board" in url_lower and "book" in url_lower:
            score -= 40
        elif "board" in url_lower:
            score -= 25
        
        # Prefer full annual report books over sections
        if "annualreportbook" in url_lower.replace("-", "").replace("_", ""):
            score -= 30
        elif "annual" in url_lower and "report" in url_lower:
            score -= 20
        
        # Prefer investment sections
        if "investment" in url_lower:
            score -= 15
        
        # CAFR/ACFR are good
        if "cafr" in url_lower or "acfr" in url_lower:
            score -= 10
        
        # Penalize partial sections (introductory, financial only)
        if "introductory" in url_lower or "intro" in url_lower:
            score += 20
        if "financialsection" in url_lower.replace("-", "").replace("_", ""):
            score += 10
        
        # Penalize old years
        if "fy20" in url_lower or "fy19" in url_lower or "fy18" in url_lower:
            score += 30
        if "2020" in url_lower or "2019" in url_lower or "2018" in url_lower:
            score += 30
            
        return score
    
    pdf_urls = sorted(unique_urls(pdf_urls), key=pdf_priority)
    
    logger.info(f"PDF URLs sorted by priority: {pdf_urls[:5]}")
    
    # Fetch up to 3 PDFs (they can be large)
    # PDF content is HIGH VALUE - put it at the FRONT of report_texts
    pdf_texts = []
    logger.info(f"Found {len(pdf_urls)} PDF URLs, fetching top 3")
    for pdf_url in pdf_urls[:3]:
        logger.info(f"Fetching PDF: {pdf_url}")
        txt = extract_text(pdf_url)
        if txt:
            pdf_texts.append(txt)
            logger.info(f"Extracted {len(txt)} chars from PDF")

    # PDF content goes FIRST (highest value), then HTML report pages
    all_report_texts = pdf_texts + report_texts

    return {
        "about_text": trim_text(" ".join(about_texts), MAX_TEXT_CHARS),
        "policy_text": trim_text(" ".join(policy_texts), MAX_TEXT_CHARS),
        "report_text": trim_text(" ".join(all_report_texts), MAX_REPORT_TEXT_CHARS)
    }


def collect_web_text_from_url(website: str) -> dict:
    """
    Convenience function to collect web text directly from a URL.
    
    Args:
        website: Website URL to scrape
        
    Returns:
        dict with keys: about_text, policy_text, report_text
    """
    fake_page = {
        "properties": {
            "Main Website": {"type": "url", "url": website}
        }
    }
    return collect_web_text(fake_page)
