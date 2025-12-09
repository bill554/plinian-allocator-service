"""
Clay API client for contact enrichment.

Integrates with Clay's webhook-based enrichment workflow:
1. Pushes contacts to Clay via webhook
2. Clay enriches (finds work email, etc.)
3. Clay calls back to Railway with enriched data

This module handles the outbound push to Clay.
The inbound webhook from Clay is handled by a separate endpoint in main.py.
"""

import logging
import httpx
from typing import Optional
from .config import SETTINGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Clay webhook URL for the "Find People" table
# This should be configured in environment variables
CLAY_FIND_PEOPLE_WEBHOOK = "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-d1314276-05c8-4be6-84ba-44bae195c0f2"


def get_property_value(page: dict, property_name: str, default: str = "") -> str:
    """
    Extract a property value from a Notion page object.
    Handles different property types (title, rich_text, url, etc.)
    """
    props = page.get("properties", {})
    
    if property_name not in props:
        return default
    
    prop = props[property_name]
    prop_type = prop.get("type")
    
    if prop_type == "title":
        titles = prop.get("title", [])
        return titles[0].get("plain_text", default) if titles else default
    
    elif prop_type == "rich_text":
        texts = prop.get("rich_text", [])
        return texts[0].get("plain_text", default) if texts else default
    
    elif prop_type == "url":
        return prop.get("url", default) or default
    
    elif prop_type == "email":
        return prop.get("email", default) or default
    
    elif prop_type == "select":
        select = prop.get("select")
        return select.get("name", default) if select else default
    
    return default


def push_to_clay_find_people(
    notion_page_id: str,
    firm_name: str,
    domain: str,
    firm_type: str = "",
    location: str = "",
    webhook_url: str = None
) -> dict:
    """
    Push a firm to Clay's "Find People" table to discover contacts.
    
    Args:
        notion_page_id: The Notion page ID of the firm (for callback reference)
        firm_name: Name of the firm
        domain: Website domain (e.g., "calpers.ca.gov")
        firm_type: Type of allocator (E&F, Pension, etc.)
        location: Geographic location
        webhook_url: Optional custom Clay webhook URL
        
    Returns:
        dict with status and response details
    """
    url = webhook_url or CLAY_FIND_PEOPLE_WEBHOOK
    
    payload = {
        "notion_page_id": notion_page_id,
        "firm_name": firm_name,
        "domain": domain,
        "firm_type": firm_type,
        "location": location,
    }
    
    logger.info(f"Pushing to Clay Find People: {firm_name} ({domain})")
    
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            logger.info(f"Successfully pushed {firm_name} to Clay")
            return {
                "success": True,
                "status_code": response.status_code,
                "firm_name": firm_name
            }
            
    except httpx.HTTPStatusError as e:
        logger.error(f"Clay HTTP error for {firm_name}: {e.response.status_code}")
        return {
            "success": False,
            "error": f"HTTP {e.response.status_code}",
            "firm_name": firm_name
        }
    except Exception as e:
        logger.error(f"Clay error for {firm_name}: {e}")
        return {
            "success": False,
            "error": str(e),
            "firm_name": firm_name
        }


def push_contact_to_clay_enrich(
    notion_page_id: str,
    name: str,
    company: str,
    linkedin_url: str = "",
    title: str = "",
    webhook_url: str = None
) -> dict:
    """
    Push a specific contact to Clay's "Enrich Contact" table for email lookup.
    
    Args:
        notion_page_id: The Notion page ID of the prospect (for callback update)
        name: Full name of the contact
        company: Company name
        linkedin_url: LinkedIn profile URL
        title: Job title
        webhook_url: Optional custom Clay webhook URL
        
    Returns:
        dict with status and response details
    """
    # This webhook URL would be for a separate "Enrich Contact" table
    # Default to find people if not specified
    url = webhook_url or CLAY_FIND_PEOPLE_WEBHOOK
    
    payload = {
        "notion_page_id": notion_page_id,
        "name": name,
        "company": company,
        "linkedin_url": linkedin_url,
        "title": title,
    }
    
    logger.info(f"Pushing to Clay Enrich Contact: {name} at {company}")
    
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            logger.info(f"Successfully pushed {name} to Clay")
            return {
                "success": True,
                "status_code": response.status_code,
                "name": name
            }
            
    except httpx.HTTPStatusError as e:
        logger.error(f"Clay HTTP error for {name}: {e.response.status_code}")
        return {
            "success": False,
            "error": f"HTTP {e.response.status_code}",
            "name": name
        }
    except Exception as e:
        logger.error(f"Clay error for {name}: {e}")
        return {
            "success": False,
            "error": str(e),
            "name": name
        }


def extract_domain_from_url(url: str) -> str:
    """
    Extract the domain from a URL.
    e.g., "https://www.calpers.ca.gov/page" -> "calpers.ca.gov"
    """
    if not url:
        return ""
    
    # Add scheme if missing
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        
        return domain
    except Exception:
        return ""


def enrich_with_clay(page: dict) -> list:
    """
    Enrich allocator with contact data from Clay.
    
    This pushes the firm to Clay's "Find People" webhook, which will:
    1. Search for investment team members
    2. Enrich with emails via waterfall
    3. Call back to Railway with discovered contacts
    
    Args:
        page: Notion page object with allocator/firm data
        
    Returns:
        list of results (empty immediately; contacts come via callback)
    """
    page_id = page.get("id", "")
    
    # Try to extract firm details from the page
    firm_name = get_property_value(page, "Firm Name") or get_property_value(page, "Name")
    website = get_property_value(page, "Website") or get_property_value(page, "Main Website")
    firm_type = get_property_value(page, "Firm Type") or get_property_value(page, "Type")
    location = get_property_value(page, "Location / Headquarters Location") or get_property_value(page, "Location")
    
    if not firm_name:
        logger.warning(f"No firm name found for page {page_id}")
        return []
    
    domain = extract_domain_from_url(website)
    
    if not domain:
        logger.warning(f"No domain found for {firm_name}")
        return []
    
    # Push to Clay - contacts will come back via webhook callback
    result = push_to_clay_find_people(
        notion_page_id=page_id,
        firm_name=firm_name,
        domain=domain,
        firm_type=firm_type,
        location=location
    )
    
    # Return empty list - actual contacts arrive via async callback
    # The result here is just for logging/tracking the push
    return [result] if result.get("success") else []
