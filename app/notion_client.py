from notion_client import Client
from .config import SETTINGS

notion = Client(auth=SETTINGS.notion_api_key)


def get_allocator_record(page_id: str):
    return notion.pages.retrieve(page_id=page_id)


def update_page_properties(page_id: str, props: dict):
    notion.pages.update(page_id=page_id, properties=props)


def query_allocators_needing_research(limit=20):
    resp = notion.databases.query(
        database_id=SETTINGS.allocators_db_id,
        filter={
            "or": [
                {"property": "Last Research Run", "date": {"is_empty": True}},
                {"property": "Last Research Run", "date": {"before": "2023-01-01"}}
            ]
        },
        page_size=limit
    )
    return resp["results"]
