from fastapi import FastAPI, Request
from .allocator_pipeline import run_batch_allocator_research
from .config import SETTINGS
from .notion_contacts import upsert_contact_for_allocator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "v15"  # Update this with each deployment

app = FastAPI(title="Plinian Allocator Service")


@app.get("/health")
def health():
    return {"status": "ok", "env": SETTINGS.env, "version": VERSION}


@app.post("/jobs/run-nightly-allocator-research")
def run_nightly():
    """Run batch allocator research job."""
    processed = run_batch_allocator_research(limit=SETTINGS.batch_limit)
    return {"processed": processed}


@app.post("/webhook/clay/person-enriched")
async def clay_person_enriched(request: Request):
    """
    Webhook callback from Clay after contact enrichment.
    
    Expected payload:
    {
        "notion_page_id": "xxx",  # Optional - if provided, updates existing page
        "name": "John Smith",
        "email": "john@example.com",
        "linkedin_url": "https://linkedin.com/in/johnsmith",
        "title": "Director of Investments",
        "company_name": "CalPERS"
    }
    """
    try:
        data = await request.json()
        logger.info(f"Received Clay webhook: {data.get('name', 'unknown')}")
        
        name = data.get("name")
        if not name:
            return {"error": "No name provided"}, 400
        
        notion_page_id = data.get("notion_page_id")
        
        # Build contact data
        contact_data = {
            "name": name,
            "email": data.get("email"),
            "linkedin_url": data.get("linkedin_url"),
            "title": data.get("title"),
            "company": data.get("company_name"),
        }
        
        # If we have a page ID, this is an update to existing prospect
        if notion_page_id and len(notion_page_id) > 10:
            # Update existing page
            from notion_client import Client
            notion = Client(auth=SETTINGS.notion_api_key)
            
            properties = {}
            
            if contact_data.get("email"):
                properties["Email"] = {"email": contact_data["email"]}
            
            if contact_data.get("linkedin_url"):
                properties["LinkedIn URL"] = {"url": contact_data["linkedin_url"]}
            
            if contact_data.get("title"):
                properties["Title/Role"] = {"rich_text": [{"text": {"content": contact_data["title"]}}]}
            
            if properties:
                notion.pages.update(page_id=notion_page_id, properties=properties)
                logger.info(f"Updated existing prospect: {name}")
                return {"status": "updated", "page_id": notion_page_id, "name": name}
        
        # Otherwise create new contact (needs allocator_id)
        allocator_id = data.get("allocator_id") or data.get("firm_page_id")
        if allocator_id:
            result_id = upsert_contact_for_allocator(allocator_id, contact_data)
            logger.info(f"Created/updated contact: {name} -> {result_id}")
            return {"status": "created", "page_id": result_id, "name": name}
        
        return {"status": "skipped", "reason": "No page_id or allocator_id provided", "name": name}
        
    except Exception as e:
        logger.error(f"Clay webhook error: {e}")
        return {"error": str(e)}, 500


@app.post("/enrich-firm")
async def enrich_single_firm(request: Request):
    """
    Trigger enrichment for a single firm by page ID.
    
    Query params:
        firm_id: Notion page ID of the firm to enrich
    """
    from .allocator_pipeline import run_allocator
    from .notion_client import get_allocator_record
    
    data = await request.json() if request.headers.get("content-type") == "application/json" else {}
    firm_id = data.get("firm_id") or request.query_params.get("firm_id")
    
    if not firm_id:
        return {"error": "firm_id required"}, 400
    
    try:
        page = get_allocator_record(firm_id)
        success = run_allocator(page)
        return {"status": "success" if success else "failed", "firm_id": firm_id}
    except Exception as e:
        logger.error(f"Enrich firm error: {e}")
        return {"error": str(e)}, 500


@app.get("/test-scrape")
async def test_scrape(url: str = "https://investments.yale.edu"):
    """Test the web scraping functionality."""
    from .web_collect import collect_web_text_from_url
    
    result = collect_web_text_from_url(url)
    return {
        "url": url,
        "about_text_length": len(result.get("about_text", "")),
        "policy_text_length": len(result.get("policy_text", "")),
        "report_text_length": len(result.get("report_text", "")),
        "about_preview": result.get("about_text", "")[:500],
    }


@app.get("/debug-enrich")
async def debug_enrich(name: str, domain: str = None):
    """
    Debug endpoint: Run full enrichment pipeline and return all intermediate data.
    Does NOT write to Notion - just shows what would be extracted.
    
    Usage: /debug-enrich?name=Indiana%20Public%20Retirement%20System&domain=in.gov/inprs
    """
    from .web_search import enrich_allocator_with_search
    from .web_collect import collect_web_text
    from .llm_jobs import call_enrich_allocator_profile
    
    result = {
        "allocator_name": name,
        "domain": domain,
        "search_results": None,
        "scraped_text_lengths": None,
        "search_snippets": None,
        "pdf_urls_found": None,
        "llm_output": None,
        "errors": []
    }
    
    try:
        # Step 1: Web search
        search_results = enrich_allocator_with_search(name, domain)
        result["search_results"] = {
            "investments_url": search_results.get("investments_url"),
            "annual_report_url": search_results.get("annual_report_url"),
            "about_url": search_results.get("about_url"),
            "team_url": search_results.get("team_url"),
        }
        result["search_snippets"] = search_results.get("search_snippets", [])
        result["pdf_urls_found"] = search_results.get("pdf_urls", [])
    except Exception as e:
        result["errors"].append(f"Search error: {e}")
        search_results = {}
    
    try:
        # Step 2: Web scraping with discovered URLs
        fake_page = {
            "properties": {
                "Main Website": {"type": "url", "url": f"https://{domain}" if domain else None}
            }
        }
        texts = collect_web_text(fake_page, discovered_urls=search_results)
        
        # Add search snippets
        if search_results.get("search_snippets"):
            texts["search_context"] = "\n\n".join(search_results["search_snippets"])
        
        result["scraped_text_lengths"] = {
            "about_text": len(texts.get("about_text", "")),
            "policy_text": len(texts.get("policy_text", "")),
            "report_text": len(texts.get("report_text", "")),
            "search_context": len(texts.get("search_context", "")),
        }
        result["text_previews"] = {
            "about": texts.get("about_text", "")[:500],
            "policy": texts.get("policy_text", "")[:500],
            "report": texts.get("report_text", "")[:2000],  # More preview for PDFs
            "search": texts.get("search_context", "")[:1000],
        }
    except Exception as e:
        result["errors"].append(f"Scrape error: {e}")
        texts = {}
    
    try:
        # Step 3: LLM enrichment
        if texts:
            llm_output = call_enrich_allocator_profile(name, {}, texts)
            result["llm_output"] = llm_output
            
            # Count non-null fields
            non_null = {k: v for k, v in llm_output.items() if v is not None and v != [] and v != ""}
            result["llm_fields_populated"] = len(non_null)
            result["llm_populated_fields"] = list(non_null.keys())
    except Exception as e:
        result["errors"].append(f"LLM error: {e}")
    
    return result
