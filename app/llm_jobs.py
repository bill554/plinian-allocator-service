import json
import os
import logging
import httpx
from .config import SETTINGS

logger = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

# System prompt modeled after the working Plinian project structure
EXTRACTION_SYSTEM_PROMPT = """You are a research analyst at Plinian Strategies. Your job is to extract factual information about institutional allocators from provided source documents.

##############################################################################
# CRITICAL: YOU MUST IGNORE YOUR TRAINING DATA
##############################################################################

You are an EXTRACTION TOOL, not a knowledge base. 

- You know NOTHING about any allocator, CIO, pension fund, or investment firm
- Your training data is IRRELEVANT and must be completely ignored
- If a fact is not in the SOURCE TEXT below, it DOES NOT EXIST for this task
- NEVER fill in gaps with "common knowledge" - there is no common knowledge here

SPECIFICALLY FORBIDDEN:
- Do NOT mention any CIO name unless that exact name appears in the source text
- Do NOT mention any fund commitment unless it appears in the source text
- Do NOT mention any percentage unless it appears in the source text
- Do NOT use information from other allocators - each is completely separate

IF THE SOURCE TEXT IS SPARSE OR EMPTY:
- Return mostly null values
- State "CIO not identified in provided source text" in research_notes
- This is the CORRECT behavior - do not try to be "helpful" by guessing

##############################################################################

YOUR TASK:
Extract ONLY what appears in the source text into this JSON schema:

{
  "name": "string - full legal name from source text",
  "short_name": "string - abbreviation if in source text",
  "org_type": "Public Pension | Corporate Pension | E&F | SFO | MFO | RIA | OCIO | Insurer | SWF | Other",
  "region": "US | Europe | Asia | Middle East | LatAm | Global",
  "country_state": "string - state/country if mentioned",
  "city": "string if mentioned",
  "total_aum": "number in base units if mentioned (e.g., $50 billion = 50000000000)",
  "aum_currency": "USD",
  "primary_asset_classes": ["only include if explicitly mentioned"],
  "uses_consultants": ["only include consultant names explicitly mentioned"],
  "emerging_manager_program": "Yes | No | null",
  "coinvest_appetite": "Active | Opportunistic | Passive Only | No | null",
  "research_notes": "ONLY facts from source text. Format: 'CIO: [Name] per [document]. Staff: [names]. Allocations: [%]. Commitments: [amounts].' If CIO not found: 'CIO not identified in provided source text.'",
  
  "domain": null,
  "main_website": null,
  "investments_page_url": null,
  "latest_report_url": null,
  "alternatives_aum": null,
  "check_size_low": null,
  "check_size_high": null,
  "check_size_notes": null,
  "geographic_focus": [],
  "investment_themes": [],
  "stage_preference": [],
  "risk_role": null,
  "em_program_details": null,
  "decision_process_summary": null,
  "coinvest_program_notes": null,
  "coinvest_decision_speed": null,
  "requires_gp_relationship": null,
  "coinvest_preferred_sectors": [],
  "coinvest_excluded_sectors": [],
  "coinvest_sector_notes": null,
  "coinvest_min_ebitda": null,
  "coinvest_max_ebitda": null,
  "coinvest_min_revenue": null,
  "coinvest_ev_range_text": null,
  "coinvest_check_size_low": null,
  "coinvest_check_size_high": null,
  "coinvest_check_size_notes": null,
  "coinvest_stake_preference": [],
  "coinvest_board_seat_requirements": null,
  "coinvest_governance_notes": null,
  "coinvest_deal_type_preference": [],
  "coinvest_geographic_preference": [],
  "avoid_non_partner_led_deals": null,
  "coinvest_rights_required": null,
  "coinvest_target_irr_range": null,
  "coinvest_target_moic_range": null,
  "coinvest_risk_tolerance_deal": null
}
}

ALLOWED VALUES FOR CONSULTANTS (only use if exact name appears in text):
Mercer, WTW, Aon, Cambridge Associates, Russell Investments, Callan Associates, NEPC, Meketa Investment Group, Wilshire Associates, Verus, Aksia, Cliffwater, Pavilion, Strategic Investment Group, Fund Evaluation Group, Albourne Partners, Stepstone Group, Hamilton Lane, bfinance, Redington, Cardano, SEI, Marquette Associates, Townsend Group

Output valid JSON only. No markdown code fences. No explanation before or after."""


def call_claude(user_content: str) -> dict:
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
        "system": EXTRACTION_SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": user_content}
        ]
    }
    
    resp = httpx.post(ANTHROPIC_API_URL, headers=headers, json=payload, timeout=90)
    resp.raise_for_status()
    
    data = resp.json()
    content = data["content"][0]["text"]
    
    # Parse JSON from response
    content = content.strip()
    if content.startswith("```json"):
        content = content[7:]
    if content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]
    
    return json.loads(content.strip())


def call_enrich_allocator_profile(allocator_name, existing_profile, texts):
    """
    Extract allocator profile from source texts using Claude.
    Structured to prevent hallucination by grounding in source text.
    """
    
    report_text = texts.get("report_text", "")
    search_context = texts.get("search_context", "")
    about_text = texts.get("about_text", "")
    policy_text = texts.get("policy_text", "")
    
    # Build the user message with clear source labeling
    user_content = f"""ALLOCATOR TO RESEARCH: {allocator_name}

=== SOURCE TEXT BEGINS ===

[SOURCE: Board Book / Annual Report / CAFR]
{report_text[:45000]}

[SOURCE: Website - About Page]
{about_text[:3000]}

[SOURCE: Website - Investment Policy]
{policy_text[:3000]}

[SOURCE: News Articles / Industry Publications]
{search_context[:6000]}

=== SOURCE TEXT ENDS ===

Extract the profile for {allocator_name} using ONLY the source text above.
Remember: If you cannot find specific information (like CIO name) in the text above, use null. Do not use any external knowledge."""

    logger.info(f"Processing {allocator_name} with {len(user_content)} chars of source text")

    try:
        logger.info("Calling Claude for extraction")
        result = call_claude(user_content)
        
        populated = len([k for k,v in result.items() if v and v != [] and v != "null"])
        logger.info(f"Claude returned {populated} non-null fields")
        
        # Log research_notes for debugging
        if result.get("research_notes"):
            logger.info(f"Research notes: {result['research_notes'][:500]}")
        
        return result
        
    except Exception as e:
        logger.error(f"Claude extraction failed: {e}", exc_info=True)
        # Return minimal result on failure
        return {
            "name": allocator_name,
            "research_notes": f"Extraction failed: {str(e)}",
            "org_type": None,
            "total_aum": None
        }
