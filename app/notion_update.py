from notion_client import Client
from .config import SETTINGS
from .mapping_config import ALLOCATOR_FIELD_CONFIG
from .notion_mapping import build_notion_property

notion = Client(auth=SETTINGS.notion_api_key)


def update_allocator_from_llm(page_id: str, enriched: dict):
    properties = {}

    for key, cfg in ALLOCATOR_FIELD_CONFIG.items():
        if key not in enriched:
            continue

        notion_name = cfg["notion_name"]
        field_type = cfg["type"]

        prop = build_notion_property(field_type, enriched.get(key))
        if prop:
            properties[notion_name] = prop

    if properties:
        notion.pages.update(page_id=page_id, properties=properties)
