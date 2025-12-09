from notion_client import Client
from .config import SETTINGS
from .notion_client import query_allocators_needing_research, get_allocator_record
from .llm_jobs import call_enrich_allocator_profile
from .notion_update import update_allocator_from_llm
from .snapshots import log_snapshot
from .web_collect import collect_web_text
from .clay_client import enrich_with_clay


def run_allocator(page):
    allocator_id = page["id"]
    name = page["properties"]["Name"]["title"][0]["plain_text"]

    try:
        # STEP 1: Web text
        texts = collect_web_text(page)

        # STEP 2: LLM Structuring
        enriched = call_enrich_allocator_profile(name, {}, texts)

        # STEP 3: Write into Notion
        update_allocator_from_llm(allocator_id, enriched)

        # STEP 4: Clay People
        clay_people = enrich_with_clay(page)
        # Add contact extraction LLM if needed

        # STEP 5: Snapshot
        log_snapshot(
            allocator_id,
            status="Success",
            input_sources={"web": True, "clay": True},
            summary=enriched.get("research_notes"),
            raw_json=enriched
        )
        return True

    except Exception as e:
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
    count = 0
    for page in allocators:
        if run_allocator(page):
            count += 1
    return count
