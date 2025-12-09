from notion_client import Client
from .config import SETTINGS
from .mapping_config import ALLOCATOR_FIELD_CONFIG
from .notion_mapping import build_notion_property
import logging

logger = logging.getLogger(__name__)

notion = Client(auth=SETTINGS.notion_api_key)


def update_allocator_from_llm(page_id: str, enriched: dict):
    properties = {}

    for key, cfg in ALLOCATOR_FIELD_CONFIG.items():
        if key not in enriched:
            continue
        
        value = enriched.get(key)
        if value is None or value == [] or value == "":
            continue

        notion_name = cfg["notion_name"]
        field_type = cfg["type"]

        prop = build_notion_property(field_type, value)
        if prop:
            properties[notion_name] = prop
            logger.info(f"Mapping {key} -> {notion_name}: {value}")

    logger.info(f"Updating Notion page {page_id} with {len(properties)} properties: {list(properties.keys())}")
    
    if properties:
        try:
            notion.pages.update(page_id=page_id, properties=properties)
            logger.info(f"Successfully updated {page_id}")
        except Exception as e:
            logger.error(f"Notion update failed: {e}")
            raise
