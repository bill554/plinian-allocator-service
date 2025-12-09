def build_notion_property(field_type: str, value):
    if value is None:
        return None

    if field_type == "title":
        return {"title": [{"text": {"content": str(value)}}]}

    if field_type == "rich_text":
        return {"rich_text": [{"text": {"content": str(value)}}]}

    if field_type == "number":
        return {"number": float(value)}

    if field_type == "select":
        return {"select": {"name": str(value)}}

    if field_type == "multi_select":
        if not isinstance(value, list):
            return None
        return {"multi_select": [{"name": str(v)} for v in value]}

    if field_type == "url":
        return {"url": str(value)}

    if field_type == "email":
        return {"email": str(value)}

    if field_type == "phone":
        return {"phone_number": str(value)}

    if field_type == "checkbox":
        return {"checkbox": bool(value)}

    return {"rich_text": [{"text": {"content": str(value)}}]}
