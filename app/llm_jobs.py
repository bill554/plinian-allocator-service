import json
import os
from openai import OpenAI
from .config import SETTINGS

client = OpenAI(api_key=SETTINGS.openai_api_key)


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
    })

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_content}
        ],
        response_format={"type": "json_object"}
    )

    return json.loads(resp.choices[0].message.content)
