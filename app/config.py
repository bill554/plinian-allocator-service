import os
from dataclasses import dataclass


@dataclass
class Settings:
    notion_api_key: str = os.getenv("NOTION_API_KEY", "")
    allocators_db_id: str = os.getenv("ALLOCATORS_DB_ID", "")
    contacts_db_id: str = os.getenv("CONTACTS_DB_ID", "")
    snapshots_db_id: str = os.getenv("SNAPSHOTS_DB_ID", "")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    clay_api_key: str = os.getenv("CLAY_API_KEY", "")
    search_api_key: str = os.getenv("SEARCH_API_KEY", "")
    env: str = os.getenv("ENV", "prod")
    batch_limit: int = int(os.getenv("BATCH_LIMIT", "20"))


SETTINGS = Settings()
