from notion_client import Client
from .config import SETTINGS

notion = Client(auth=SETTINGS.notion_api_key)


def log_snapshot(allocator_id: str, status: str, input_sources: dict, summary: str, raw_json: dict, error: str = None):
    notion.pages.create(
        parent={"database_id": SETTINGS.snapshots_db_id},
        properties={
            "Allocator": {"relation": [{"id": allocator_id}]},
            "Status": {"select": {"name": status}},
            "Input Sources": {"rich_text": [{"text": {"content": str(input_sources)}}]},
            "Extracted Summary": {"rich_text": [{"text": {"content": summary or ""}}]},
            "Raw LLM JSON": {"rich_text": [{"text": {"content": str(raw_json)[:1800]}}]},
            "Error Message": {"rich_text": [{"text": {"content": error or ""}}]}
        }
    )
