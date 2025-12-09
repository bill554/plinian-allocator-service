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

logger = logging.getLogger(__name__)


def extract_domain(page) -> str:
    """Extract domain from page properties."""
    props = page.get("properties", {})
    
    # Try Main Website first
    website = props.get("Main Website", {}).get("url")
    if website:
        from urllib.parse import urlparse
        parsed = urlparse(website)
        return parsed.netloc.replace("www.", "")
    
    # Try Domain field
    domain_prop = props.get("Domain", {}).get("rich_text", [])
    if domain_prop:
        return domain_prop[0].get("plain_text", "").replace("www.", "")
    
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
