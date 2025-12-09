from notion_client import Client
from .config import SETTINGS
import logging

logger = logging.getLogger(__name__)

notion = Client(auth=SETTINGS.notion_api_key)


def log_snapshot(allocator_id: str, status: str, input_sources: dict, summary: str, raw_json: dict, error: str = None):
    if not SETTINGS.snapshots_db_id:
        logger.info(f"Snapshot logging skipped (no SNAPSHOTS_DB_ID): {status} for {allocator_id}")
        return
    
    try:
        notion.pages.create(
            parent={"database_id": SETTINGS.snapshots_db_id},
            properties={
                "Allocator": {"relation": [{"id": allocator_id}]},
                "Status": {"select": {"name": status}},
                "Input Sources": {"rich_text": [{"text": {"content": str(input_sources)[:2000]}}]},
                "Extracted Summary": {"rich_text": [{"text": {"content": (summary or "")[:2000]}}]},
                "Raw LLM JSON": {"rich_text": [{"text": {"content": str(raw_json)[:1800]}}]},
                "Error Message": {"rich_text": [{"text": {"content": (error or "")[:2000]}}]}
            }
        )
    except Exception as e:
        logger.warning(f"Failed to log snapshot: {e}")
