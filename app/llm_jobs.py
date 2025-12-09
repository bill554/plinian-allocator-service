import json
import os
import logging
import httpx
from .config import SETTINGS

logger = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

# System prompt modeled after the working Plinian project structure
EXTRACTION_SYSTEM_PROMPT = """You are a research analyst at Plinian Strategies, a boutique capital-raising and advisory firm. Your job is to extract factual information about institutional allocators from provided source documents.

CRITICAL GROUNDING RULES:
1. You may ONLY extract information that appears in the PROVIDED SOURCE TEXT below
2. You have NO knowledge of any allocators, CIOs, or investment data from your training
3. If information is not in the source text, you MUST use null
4. Do NOT guess, infer, or fill in gaps
5. Do NOT confuse this allocator with any other organization

VERIFICATION REQUIREMENT:
For key facts (CIO name, AUM, commitments), you must be able to point to the exact phrase in the source text. If you cannot find the exact phrase, use null.

YOUR TASK:
Extract allocator profile data into the JSON schema below. Only populate fields where you find explicit evidence in the source text.

JSON SCHEMA:
{
  "name": "string - full legal name of the organization",
  "short_name": "string - common abbreviation (e.g., INPRS, CalPERS)",
  "org_type": "one of: Public Pension | Corporate Pension | E&F | SFO | MFO | RIA | OCIO | Insurer | SWF | Other",
  "region": "US | Europe | Asia | Middle East | LatAm | Global",
  "country_state": "string - US state or country",
  "city": "string",
  "total_aum": "number in base units (e.g., $50 billion = 50000000000)",
  "aum_currency": "USD unless otherwise specified",
  "primary_asset_classes": ["array - only include if mentioned: Public Equity, Public Fixed Income, Private Equity, Private Credit, Real Estate, Real Assets, Hedge Funds, Multi-Asset, Risk Parity, Commodities"],
  "uses_consultants": ["array - only include consultant names explicitly mentioned: Verus, NEPC, Callan, Mercer, Cambridge Associates, Meketa, etc."],
  "emerging_manager_program": "Yes | No | null if not mentioned",
  "coinvest_appetite": "Active | Opportunistic | Passive Only | No | null if not mentioned",
  "research_notes": "string - key facts with source attribution. Format: 'CIO: [Name] per [source]. Staff: [names/titles]. Allocations: [percentages]. Commitments: [amounts/funds].' If CIO not found, state 'CIO not identified in source text.'",
  
  // Leave these null unless explicitly found:
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
