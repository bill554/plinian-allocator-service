from notion_client import Client
from .config import SETTINGS
from .contact_mapping_config import CONTACT_FIELD_CONFIG
from .notion_mapping import build_notion_property

notion = Client(auth=SETTINGS.notion_api_key)


def find_contact(allocator_id: str, name: str):
    results = notion.databases.query(
        database_id=SETTINGS.contacts_db_id,
        filter={
            "and": [
                {"property": "Allocator", "relation": {"contains": allocator_id}},
                {"property": "Name", "title": {"equals": name}}
            ]
        }
    )
    if results["results"]:
        return results["results"][0]["id"]
    return None


def upsert_contact_for_allocator(allocator_id: str, contact_data: dict, clay_person_id=None):
    name = contact_data.get("name")
    if not name:
        return None

    existing_id = find_contact(allocator_id, name)

    properties = {
        "Allocator": {"relation": [{"id": allocator_id}]}
    }

    if clay_person_id:
        properties["Clay Person ID"] = {"rich_text": [{"text": {"content": clay_person_id}}]}

    for key, cfg in CONTACT_FIELD_CONFIG.items():
        notion_name = cfg["notion_name"]
        field_type = cfg["type"]
        value = contact_data.get(key)
        prop = build_notion_property(field_type, value)
        if prop:
            properties[notion_name] = prop

    if existing_id:
        notion.pages.update(page_id=existing_id, properties=properties)
        return existing_id
    else:
        page = notion.pages.create(
            parent={"database_id": SETTINGS.contacts_db_id},
            properties=properties
        )
        return page["id"]
