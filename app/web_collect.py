"""
Web text collection for allocator research.

Scrapes allocator websites to extract relevant text from:
- About/Overview pages
- Investment/Portfolio pages  
- Policy documents and RFPs
- Annual reports, CAFRs, and board materials

Supports both HTML and PDF extraction.
"""

import re
from io import BytesIO
from urllib.parse import urljoin

import httpx
import trafilatura
from pypdf import PdfReader


DEFAULT_TIMEOUT = 12  # seconds
MAX_TEXT_CHARS = 12000  # per bucket - increased for more comprehensive data
MAX_URLS_PER_BUCKET = 10  # rate limiting - increased slightly


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
            resp = client.get(url)
        if resp.status_code != 200:
            return "", ""

        content_type = resp.headers.get("content-type", "").lower()
        # For PDFs, we need resp.content (bytes); for HTML, resp.text
        is_pdf = "pdf" in content_type or url.lower().endswith(".pdf")
        return content_type, resp.content if is_pdf else resp.text
    except Exception:
        return "", ""


# ----- 2. Helper: extract text from HTML or PDF ----- #

def extract_text_from_html(html: str) -> str:
    """Extract clean text from HTML using trafilatura."""
    if not html:
        return ""
    try:
        extracted = trafilatura.extract(html, include_comments=False)
        return extracted or ""
    except Exception:
        return ""


def extract_text_from_pdf(data: bytes) -> str:
    """Extract text from PDF bytes using pypdf."""
    if not data:
        return ""
    try:
        reader = PdfReader(BytesIO(data))
    except Exception:
        return ""

    texts = []
    # Read only first N pages to keep it manageable
    max_pages = min(len(reader.pages), 10)
    for i in range(max_pages):
        try:
            page = reader.pages[i]
            texts.append(page.extract_text() or "")
        except Exception:
            continue

    return "\n".join(texts)


def extract_text(url: str) -> str:
    """Fetch URL and extract text (handles both HTML and PDF)."""
    content_type, body = safe_get(url)
    if not content_type or body == "":
        return ""

    if "pdf" in content_type or url.lower().endswith(".pdf"):
        # PDF
        if isinstance(body, str):
            body = body.encode("utf-8", errors="ignore")
        return extract_text_from_pdf(body)
    else:
        # Assume HTML or text
        if isinstance(body, bytes):
            try:
                body = body.decode("utf-8", errors="ignore")
            except Exception:
                return ""
        return extract_text_from_html(body)


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
            {investments_url, annual_report_url, about_url, team_url}
        
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

    # Priority 1: Explicit Notion-specified URLs
    if investments_page:
        policy_urls.append(investments_page)
    if latest_report_url:
        report_urls.append(latest_report_url)
    
    # Priority 2: Search-discovered URLs
    if discovered_urls.get("investments_url"):
        policy_urls.append(discovered_urls["investments_url"])
    if discovered_urls.get("annual_report_url"):
        report_urls.append(discovered_urls["annual_report_url"])
    if discovered_urls.get("about_url"):
        about_urls.append(discovered_urls["about_url"])
    if discovered_urls.get("team_url"):
        about_urls.append(discovered_urls["team_url"])

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

    # Fetch & aggregate text (with rate limiting)
    about_texts = []
    policy_texts = []
    report_texts = []

    for url in about_urls[:MAX_URLS_PER_BUCKET]:
        txt = extract_text(url)
        if txt:
            about_texts.append(txt)

    for url in policy_urls[:MAX_URLS_PER_BUCKET]:
        txt = extract_text(url)
        if txt:
            policy_texts.append(txt)

    for url in report_urls[:MAX_URLS_PER_BUCKET]:
        txt = extract_text(url)
        if txt:
            report_texts.append(txt)

    return {
        "about_text": trim_text(" ".join(about_texts)),
        "policy_text": trim_text(" ".join(policy_texts)),
        "report_text": trim_text(" ".join(report_texts))
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
