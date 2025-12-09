import json
import os
import logging
import httpx
from .config import SETTINGS

logger = logging.getLogger(__name__)

# Try to import OpenAI, but it's now optional
try:
    from openai import OpenAI
    openai_client = OpenAI(api_key=SETTINGS.openai_api_key) if SETTINGS.openai_api_key else None
except ImportError:
    openai_client = None

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"


def call_claude(system_prompt: str, user_content: str) -> dict:
    """Call Claude API for extraction."""
    if not SETTINGS.anthropic_api_key:
        raise ValueError("ANTHROPIC_API_KEY not configured")
    
    headers = {
        "x-api-key": SETTINGS.anthropic_api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    
    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4096,
        "temperature": 0,
        "system": system_prompt,
        "messages": [
            {"role": "user", "content": user_content}
        ]
    }
    
    resp = httpx.post(ANTHROPIC_API_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    
    data = resp.json()
    content = data["content"][0]["text"]
    
    # Parse JSON from response (Claude may wrap in markdown)
    content = content.strip()
    if content.startswith("```json"):
        content = content[7:]
    if content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]
    
    return json.loads(content.strip())


def call_openai(system_prompt: str, user_content: str) -> dict:
    """Call OpenAI API for extraction."""
    if not openai_client:
        raise ValueError("OpenAI not configured")
    
    resp = openai_client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ],
        response_format={"type": "json_object"}
    )
    
    return json.loads(resp.choices[0].message.content)


def call_enrich_allocator_profile(allocator_name, existing_profile, texts):
    prompt_path = os.path.join(os.path.dirname(__file__), "prompts", "enrich_allocator_profile.txt")
    with open(prompt_path) as f:
        sys_prompt = f.read()

    user_content = json.dumps({
        "allocator_name": allocator_name,
        "existing_profile": existing_profile,
        "about_text": texts.get("about_text", ""),
        "policy_text": texts.get("policy_text", ""),
        "report_text": texts.get("report_text", ""),
        "search_context": texts.get("search_context", "")
    }, indent=2)

    logger.info(f"Calling LLM for {allocator_name} with {len(user_content)} chars of context")

    # Try Claude first (primary), fall back to OpenAI
    if SETTINGS.anthropic_api_key:
        try:
            logger.info("Using Claude for extraction")
            result = call_claude(sys_prompt, user_content)
            logger.info(f"Claude returned {len([k for k,v in result.items() if v])} non-null fields")
            return result
        except Exception as e:
            logger.error(f"Claude failed: {e}")
            if openai_client:
                logger.info("Falling back to OpenAI")
    
    # Fallback to OpenAI
    if openai_client:
        logger.info("Using OpenAI for extraction")
        result = call_openai(sys_prompt, user_content)
        logger.info(f"OpenAI returned {len([k for k,v in result.items() if v])} non-null fields")
        return result
    
    raise ValueError("No LLM API configured (need ANTHROPIC_API_KEY or OPENAI_API_KEY)")
