"""
Web search module to find investment-related pages for allocators.
Uses Serper API (Google Search) to discover URLs before scraping.
"""
import httpx
import logging
from .config import SETTINGS

logger = logging.getLogger(__name__)

SERPER_ENDPOINT = "https://google.serper.dev/search"


def search_google(query: str, num_results: int = 10) -> list[dict]:
    """
    Search Google via Serper API.
    Returns list of {title, link, snippet} dicts.
    """
    if not SETTINGS.search_api_key:
        logger.warning("No SEARCH_API_KEY configured, skipping web search")
        return []
    
    try:
        resp = httpx.post(
            SERPER_ENDPOINT,
            headers={"X-API-KEY": SETTINGS.search_api_key, "Content-Type": "application/json"},
            json={"q": query, "num": num_results},
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("organic", [])
    except Exception as e:
        logger.error(f"Serper search failed: {e}")
        return []


def find_investment_pages(allocator_name: str, domain: str = None) -> dict:
    """
    Search for investment-related pages for an allocator.
    Returns dict with discovered URLs:
    {
        "investments_url": str or None,
        "annual_report_url": str or None,
        "about_url": str or None,
        "team_url": str or None,
        "search_snippets": list of relevant snippets
    }
    """
    result = {
        "investments_url": None,
        "annual_report_url": None,
        "about_url": None,
        "team_url": None,
        "search_snippets": []
    }
    
    # Build search queries
    queries = [
        f'"{allocator_name}" investments asset allocation',
        f'"{allocator_name}" annual report CAFR',
        f'"{allocator_name}" investment office CIO team',
    ]
    
    # If we have a domain, add site-specific searches
    if domain:
        queries.extend([
            f'site:{domain} investments',
            f'site:{domain} annual report',
            f'site:{domain} asset allocation',
        ])
    
    all_results = []
    for query in queries[:4]:  # Limit to 4 queries to save API calls
        results = search_google(query, num_results=5)
        all_results.extend(results)
        logger.info(f"Search '{query}' returned {len(results)} results")
    
    # Deduplicate by URL
    seen_urls = set()
    unique_results = []
    for r in all_results:
        url = r.get("link", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_results.append(r)
    
    # Categorize results
    for r in unique_results:
        url = r.get("link", "").lower()
        title = r.get("title", "").lower()
        snippet = r.get("snippet", "")
        
        # Collect relevant snippets for LLM context
        if any(kw in snippet.lower() for kw in ["billion", "million", "asset", "allocation", "portfolio", "aum", "cio", "investment", "committed", "private equity", "real estate", "hedge", "consultant"]):
            result["search_snippets"].append(snippet)
        
        # Categorize URL by type
        if not result["investments_url"]:
            if any(kw in url for kw in ["investment", "portfolio", "asset-allocation", "assets"]):
                result["investments_url"] = r.get("link")
            elif any(kw in title for kw in ["investment", "portfolio", "asset allocation"]):
                result["investments_url"] = r.get("link")
        
        if not result["annual_report_url"]:
            if any(kw in url for kw in ["annual-report", "annualreport", "cafr", "financial-report"]):
                result["annual_report_url"] = r.get("link")
            elif any(kw in title for kw in ["annual report", "cafr", "financial report"]):
                result["annual_report_url"] = r.get("link")
        
        if not result["about_url"]:
            if any(kw in url for kw in ["about", "who-we-are", "our-story", "overview"]):
                result["about_url"] = r.get("link")
        
        if not result["team_url"]:
            if any(kw in url for kw in ["team", "staff", "leadership", "people", "board"]):
                result["team_url"] = r.get("link")
            elif any(kw in title for kw in ["team", "staff", "leadership", "board of trustees"]):
                result["team_url"] = r.get("link")
    
    # Limit snippets
    result["search_snippets"] = result["search_snippets"][:10]
    
    logger.info(f"Found URLs for {allocator_name}: investments={result['investments_url']}, report={result['annual_report_url']}")
    
    return result


def enrich_allocator_with_search(allocator_name: str, domain: str = None) -> dict:
    """
    Main entry point: search for allocator info and return enriched context.
    Returns dict with URLs and snippets that can augment web scraping.
    """
    pages = find_investment_pages(allocator_name, domain)
    
    # Do additional targeted searches for institutional data
    additional_queries = [
        f'"{allocator_name}" private equity commitment million',
        f'"{allocator_name}" real estate real assets allocation',
        f'"{allocator_name}" consultant Verus NEPC Callan Mercer',
        f'"{allocator_name}" CIO chief investment officer',
        f'"{allocator_name}" co-investment coinvest',
        f'site:pionline.com "{allocator_name}"',  # P&I has great pension data
        f'site:top1000funds.com "{allocator_name}"',  # Top1000 funds profiles
    ]
    
    for query in additional_queries:
        results = search_google(query, num_results=5)
        for r in results:
            snippet = r.get("snippet", "")
            if snippet and len(snippet) > 50:
                # Check for high-value content
                if any(kw in snippet.lower() for kw in [
                    "billion", "million", "committed", "allocated", "allocation",
                    "private equity", "real estate", "real assets", "hedge fund",
                    "cio", "chief investment", "consultant", "verus", "nepc", "callan",
                    "co-invest", "coinvest", "direct investment"
                ]):
                    if snippet not in pages["search_snippets"]:
                        pages["search_snippets"].append(snippet)
    
    # Keep more snippets - they contain the best data
    pages["search_snippets"] = pages["search_snippets"][:20]
    
    logger.info(f"Total search snippets for {allocator_name}: {len(pages['search_snippets'])}")
    
    return pages
