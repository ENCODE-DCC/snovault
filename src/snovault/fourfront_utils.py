# Basic fourfront-specific utilities that seem to have a good home in snovault

def add_default_embeds(embeds, schema={}):
    """Perform default processing on the embeds list.
    This adds display_title to any non-fully embedded linkTo field and defaults
    to using the @id and display_title of non-embedded linkTo's
    Used in fourfront/../types/base.py AND snovault create mapping
    """
    if 'properties' in schema:
        schema = schema['properties']
    processed_fields = embeds[:] if len(embeds) > 0 else []
    already_processed = []
    # find pre-existing fields
    for field in embeds:
        split_field = field.strip().split('.')
        if len(split_field) > 1:
            embed_path = '.'.join(split_field[:-1])
            if embed_path not in processed_fields and embed_path not in already_processed:
                already_processed.append(embed_path)
                if embed_path + '.link_id' not in processed_fields:
                    processed_fields.append(embed_path + '.link_id')
                if embed_path + '.display_title' not in processed_fields:
                    processed_fields.append(embed_path + '.display_title')
    # automatically embed top level linkTo's not already embedded
    for key, val in schema.items():
        check_linkTo = 'linkTo' in val or ('items' in val and 'linkTo' in val['items'])
        if key not in processed_fields and check_linkTo:
            if key + '.link_id' not in processed_fields:
                processed_fields.append(key + '.link_id')
            if key + '.display_title' not in processed_fields:
                processed_fields.append(key + '.display_title')
    return processed_fields
