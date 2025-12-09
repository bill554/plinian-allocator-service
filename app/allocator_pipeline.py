from notion_client import Client
from .config import SETTINGS
from .notion_client import query_allocators_needing_research, get_allocator_record
from .llm_jobs import call_enrich_allocator_profile
from .notion_update import update_allocator_from_llm
from .snapshots import log_snapshot
from .web_collect import collect_web_text
from .web_search import enrich_allocator_with_search
from .clay_client import enrich_with_clay
import logging
import httpx

logger = logging.getLogger(__name__)


def resolve_final_domain(url: str) -> str:
    """Follow redirects and return the final domain."""
    if not url:
        return None
    
    try:
        # Follow redirects to get final URL
        with httpx.Client(follow_redirects=True, timeout=10) as client:
            resp = client.head(url)
            final_url = str(resp.url)
            from urllib.parse import urlparse
            parsed = urlparse(final_url)
            domain = parsed.netloc.replace("www.", "")
            logger.info(f"Resolved {url} -> {final_url} (domain: {domain})")
            return domain
    except Exception as e:
        logger.warning(f"Could not resolve {url}: {e}")
        return None


def extract_domain(page) -> str:
    """Extract domain from page properties, resolving redirects."""
    props = page.get("properties", {})
    
    # Try Main Website first
    website = props.get("Main Website", {}).get("url")
    if website:
        # Try to resolve redirects
        resolved = resolve_final_domain(website)
        if resolved:
            return resolved
        # Fallback to just parsing the URL
        from urllib.parse import urlparse
        parsed = urlparse(website)
        return parsed.netloc.replace("www.", "")
    
    # Try Domain field
    domain_prop = props.get("Domain", {}).get("rich_text", [])
    if domain_prop:
        domain = domain_prop[0].get("plain_text", "").replace("www.", "")
        # Try to resolve if it looks like a domain
        if domain and "." in domain:
            resolved = resolve_final_domain(f"https://{domain}")
            if resolved:
                return resolved
        return domain
    
    return None


def run_allocator(page):
    allocator_id = page["id"]
    props = page.get("properties", {})
    name_prop = props.get("Name", {}).get("title", [])
    name = name_prop[0]["plain_text"] if name_prop else "Unknown"
    
    logger.info(f"Processing allocator: {name} ({allocator_id})")

    try:
        domain = extract_domain(page)
        
        # STEP 0: Web Search to find investment pages
        search_results = enrich_allocator_with_search(name, domain)
        logger.info(f"Search found: investments={search_results.get('investments_url')}, report={search_results.get('annual_report_url')}")
        
        # STEP 1: Web text (enhanced with search-discovered URLs)
        texts = collect_web_text(page, discovered_urls=search_results)
        logger.info(f"Collected web text - about: {len(texts.get('about_text', ''))}, policy: {len(texts.get('policy_text', ''))}, report: {len(texts.get('report_text', ''))}")
        
        # Add search snippets to the text context
        if search_results.get("search_snippets"):
            snippet_text = "\n\n".join(search_results["search_snippets"])
            texts["search_context"] = snippet_text
            logger.info(f"Added {len(search_results['search_snippets'])} search snippets to context")

        # STEP 2: LLM Structuring
        enriched = call_enrich_allocator_profile(name, {}, texts)
        logger.info(f"LLM returned {len([k for k, v in enriched.items() if v is not None and v != []])} non-null fields")
        logger.info(f"LLM output: {enriched}")

        # STEP 3: Write into Notion
        update_allocator_from_llm(allocator_id, enriched)
        logger.info(f"Updated Notion for {name}")

        # STEP 4: Clay People
        clay_people = enrich_with_clay(page)
        # Add contact extraction LLM if needed

        # STEP 5: Snapshot
        log_snapshot(
            allocator_id,
            status="Success",
            input_sources={"web": True, "clay": True, "search": bool(search_results.get("search_snippets"))},
            summary=enriched.get("research_notes"),
            raw_json=enriched
        )
        return True

    except Exception as e:
        logger.error(f"Error processing {name}: {e}", exc_info=True)
        log_snapshot(
            allocator_id,
            status="Failed",
            input_sources={},
            summary=None,
            raw_json=None,
            error=str(e)
        )
        return False


def run_batch_allocator_research(limit=20):
    allocators = query_allocators_needing_research(limit)
    logger.info(f"Found {len(allocators)} allocators needing research")
    count = 0
    for page in allocators:
        if run_allocator(page):
            count += 1
    return count
