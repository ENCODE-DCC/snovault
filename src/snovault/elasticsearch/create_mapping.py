"""\
Example.

To load the initial data:

    %(prog)s production.ini

"""
from pyramid.paster import get_app
from elasticsearch import RequestError
from elasticsearch_dsl import Index
from elasticsearch_dsl.connections import connections
from functools import reduce
from snovault import (
    COLLECTIONS,
    TYPES,
)
from snovault.schema_utils import combine_schemas
from snovault.fourfront_utils import add_default_embeds
from .interfaces import ELASTIC_SEARCH
import collections
import json
import logging



log = logging.getLogger(__name__)


EPILOG = __doc__

log = logging.getLogger(__name__)

# An index to store non-content metadata
META_MAPPING = {
    '_all': {
        'enabled': False,
        'analyzer': 'snovault_index_analyzer',
        'search_analyzer': 'snovault_search_analyzer'
    },
    'dynamic_templates': [
        {
            'store_generic': {
                'match': '*',
                'mapping': {
                    'index': 'no',
                    'store': 'yes',
                },
            },
        },
    ],
}

PATH_FIELDS = ['submitted_file_name']
NON_SUBSTRING_FIELDS = ['uuid', '@id', 'submitted_by', 'md5sum', 'references', 'submitted_file_name']


def sorted_pairs_hook(pairs):
    return collections.OrderedDict(sorted(pairs))


def sorted_dict(d):
    return json.loads(json.dumps(d), object_pairs_hook=sorted_pairs_hook)


def schema_mapping(name, schema, field='*'):
    """
    Create the mapping for a given schema. Defaults to using all fields for
    objects (*), but can handle specific fields using the field parameter.
    This allows for the mapping to match the selective embedding.
    """
    if 'linkFrom' in schema:
        type_ = 'string'
    else:
        type_ = schema['type']

    # Elasticsearch handles multiple values for a field
    if type_ == 'array' and schema['items']:
        return schema_mapping(name, schema['items'], field)

    if type_ == 'object':
        properties = {}
        for k, v in schema.get('properties', {}).items():
            mapping = schema_mapping(k, v, '*')
            if mapping is not None:
                if field == '*' or k == field:
                    properties[k] = mapping
        return {
            'type': 'object',
            'include_in_all': True,
            'properties': properties,
        }

    if type_ == ["number", "string"]:
        return {
            'type': 'text',
            'copy_to': [],
            'index': True,
            'fields': {
                'value': {
                    'type': 'float',
                    'copy_to': '',
                    'ignore_malformed': True,
                    'copy_to': []
                },
                'raw': {
                    'type': 'keyword',
                    'index': True
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }

    if type_ == 'boolean':
        return {
            'type': 'text',
            'store': True,
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'index': True
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }

    if type_ == 'string':
        # don't make a mapping for non-embedded objects
        if 'linkTo' in schema or 'linkFrom' in schema:
            return

        sub_mapping = {
            'type': 'text',
            'store': True
        }

        if schema.get('elasticsearch_mapping_index_type'):
             if schema.get('elasticsearch_mapping_index_type')['default'] == 'analyzed':
                return sub_mapping
        else:
            sub_mapping.update({
                            'fields': {
                                'raw': {
                                    'type': 'keyword',
                                    'index': True
                                },
                                'lower_case_sort': {
                                    'type': 'text',
                                    'analyzer': 'case_insensistive_sort',
                                    'fields': {
                                        'keyword': {
                                            'type': 'keyword'
                                        }
                                    }
                                }
                            }
                        })
            # these fields are unintentially partially matching some small search
            # keywords because fields are analyzed by nGram analyzer
        if name in NON_SUBSTRING_FIELDS:
            if name in PATH_FIELDS:
                sub_mapping['index_analyzer'] = 'snovault_path_analyzer'
            else:
                sub_mapping['index'] = True
        return sub_mapping

    if type_ == 'number':
        return {
            'type': 'float',
            'store': True,
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'index': True
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }

    if type_ == 'integer':
        return {
            'type': 'long',
            'store': True,
            'fields': {
                'raw': {
                    'type': 'keyword',
                    'index': True
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }


def index_settings():
    return {
        'index': {
            'mapping.total_fields.limit': 3000,
            'number_of_shards': 1,
            'max_result_window': 100000,
            'merge': {
                'policy': {
                    'max_merged_segment': '2gb',
                    'max_merge_at_once': 5
                }
            },
            'analysis': {
                'filter': {
                    'substring': {
                        'type': 'nGram',
                        'min_gram': 1,
                        'max_gram': 33
                    }
                },
                'analyzer': {
                    'default': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'char_filter': 'html_strip',
                        'filter': [
                            'standard',
                            'lowercase',
                        ]
                    },
                    'snovault_index_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'char_filter': 'html_strip',
                        'filter': [
                            'standard',
                            'lowercase',
                            'asciifolding',
                            'substring'
                        ]
                    },
                    'snovault_search_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'filter': [
                            'standard',
                            'lowercase',
                            'asciifolding'
                        ]
                    },
                    'case_insensistive_sort': {
                        'tokenizer': 'keyword',
                        'filter': [
                            'lowercase',
                        ]
                    },
                    'snovault_path_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'snovault_path_tokenizer',
                        'filter': ['lowercase']
                    }
                },
                'tokenizer': {
                    'snovault_path_tokenizer': {
                        'type': 'path_hierarchy',
                        'reverse': True
                    }
                }
            }
        }
    }


def audit_mapping():
    return {
        'category': {
            'type': 'text',
            'index': True,
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'detail': {
            'type': 'text',
            'index': 'analyzed',
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'level_name': {
            'type': 'text',
            'index': True,
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'level': {
            'type': 'integer',
        }
    }


def es_mapping(mapping):
    return {
        '_all': {
            'enabled': True,
            'analyzer': 'snovault_index_analyzer',
            'search_analyzer': 'snovault_search_analyzer'
        },
        'dynamic_templates': [
            {
                'template_principals_allowed': {
                    'path_match': "principals_allowed.*",
                    'mapping': {
                        'type': 'text',
                        'index': True,
                    },
                },
            },
            {
                'template_unique_keys': {
                    'path_match': "unique_keys.*",
                    'mapping': {
                        'type': 'text',
                        'index': True,
                    },
                },
            },
            {
                'template_links': {
                    'path_match': "links.*",
                    'mapping': {
                        'type': 'text',
                        'index': True,
                    },
                },
            },
        ],
        'properties': {
            'uuid': {
                'type': 'text',
                'index': True,
                'include_in_all': False,
            },
            'tid': {
                'type': 'text',
                'index': True,
                'include_in_all': False,
            },
            'item_type': {
                'type': 'text',
                'index': True
            },
            'embedded': mapping,
            'object': {
                'type': 'object',
                'enabled': False,
                'include_in_all': False,
            },
            'properties': {
                'type': 'object',
                'enabled': False,
                'include_in_all': False,
            },
            'propsheets': {
                'type': 'object',
                'enabled': False,
                'include_in_all': False,
            },
            'principals_allowed': {
                'type': 'object',
                'include_in_all': False,
                'properties': {
                    'view': {
                        'type': 'keyword',
                        'index': True
                    },
                    'edit': {
                        'type': 'keyword',
                        'index': True
                    },
                    'audit': {
                        'type': 'keyword',
                        'index': True
                    }
                }
            },
            'embedded_uuids': {
                'type': 'text',
                'include_in_all': False,
                'index': True
            },
            'linked_uuids': {
                'type': 'text',
                'include_in_all': False,
                'index': True
            },
            'unique_keys': {
                'type': 'object',
                'include_in_all': False,
            },
            'links': {
                'type': 'object',
                'include_in_all': False,
            },
            'paths': {
                'type': 'text',
                'include_in_all': False,
                'index': True
            },
            'audit': {
                'type': 'object',
                'include_in_all': False,
                'properties': {
                    'ERROR': {
                        'type': 'object',
                        'properties': audit_mapping()
                    },
                    'NOT_COMPLIANT': {
                        'type': 'object',
                        'properties': audit_mapping()
                    },
                    'WARNING': {
                        'type': 'object',
                        'properties': audit_mapping()
                    },
                    'INTERNAL_ACTION': {
                        'type': 'object',
                        'properties': audit_mapping()
                    },
                },
            }
        }
    }


def type_mapping(types, item_type, embed=True):
    """
    Create mapping for each type. This is relatively simple if embed=False.
    When embed=True, the embedded fields (defined in /types/ directory) will
    be used to generate custom embedding of objects. Embedding paths are
    separated by dots. If the last field is an object, all fields in that
    object will be embedded (e.g. biosource.individual). To embed a specific
    field only, do add it at the end of the path: biosource.individual.title

    No field checking has been added yet (TODO?), so make sure fields are
    spelled correctly.

    Any fields that are not objects will NOT be embedded UNLESS they are in the
    embedded list, again defined in the types .py file for the object.
    """
    type_info = types[item_type]
    schema = type_info.schema
    mapping = schema_mapping(item_type, schema)
    embeds = add_default_embeds(type_info.embedded, schema)
    if not embed:
        return mapping
    for prop in embeds:
        single_embed = {}
        s = schema
        m = mapping
        for p in prop.split('.'):
            ref_types = None
            subschema = None
            ultimate_obj = False # set to true if on last level of embedding
            field = '*'
            # Check if we're at the end of a hierarchy of embeds
            if p == prop.split('.')[-1] and len(prop.split('.')) > 1:
                # See if the embedding was done improperly (last field is object)
                subschema = s.get('properties', {}).get(p)
                    # if last field is object, default to embedding all fields (*)
                if subschema is None: # Check if second to last field is object
                    subschema = s
                    ultimate_obj = True
                    field = p
            # Check if only an object was given. Embed fully (leave field = *)
            elif len(prop.split('.')) == 1:
                subschema = s.get('properties', {}).get(p)
                # if a non-obj field, return (no embedding is going on)
                if subschema is None:
                    break
            else: # in this case, field itself should be an object. If not, return
                # If last field is an object, embed it entirely
                subschema = s.get('properties', {}).get(p)
                field = p
                if subschema is None:
                    break
            # handle arrays by simply jumping into them
            # we don't care that they're flattened during mapping
            subschema = subschema.get('items', subschema)
            if 'linkFrom' in subschema:
                _ref_type, _ = subschema['linkFrom'].split('.', 1)
                ref_types = [_ref_type]
            elif 'linkTo' in subschema:
                ref_types = subschema['linkTo']
                if not isinstance(ref_types, list):
                    ref_types = [ref_types]
            if ref_types is None:
                s = subschema
            else:
                s = reduce(combine_schemas, (types[t].schema for t in ref_types))
            # Check if mapping for property is already an object
            # multiple subobjects may be embedded, so be careful here
            if ultimate_obj: # this means we're at the at the end of an embed
                m['properties'][field] = schema_mapping(p, s, field)
            elif p in m['properties']:
                if m['properties'][p]['type'] == 'string':
                    m['properties'][p] = schema_mapping(p, s, field)
                # add a field that's an object
                elif m['properties'][p]['type'] == 'object' and p != field and field != '*':
                    m['properties'][p][field] = schema_mapping(p, s, field)
            else:
                m['properties'][p] = schema_mapping(p, s, field)
            m = m['properties'][p] if not ultimate_obj else m['properties']

    # boost_values = schema.get('boost_values', None)
    # if boost_values is None:
    #     boost_values = {
    #         prop_name: 1.0
    #         for prop_name in ['@id', 'title']
    #         if prop_name in mapping['properties']
    #     }
    # for name, boost in boost_values.items():
    #     props = name.split('.')
    #     last = props.pop()
    #     new_mapping = mapping['properties']
    #     for prop in props:
    #         new_mapping = new_mapping[prop]['properties']
    #     new_mapping[last]['boost'] = boost
    #     if last in NON_SUBSTRING_FIELDS:
    #         new_mapping[last]['include_in_all'] = False
    #         if last in PATH_FIELDS:
    #             new_mapping[last]['index_analyzer'] = 'snovault_path_analyzer'
    #         else:
    #             new_mapping[last]['index'] = True
    #     else:
    #         new_mapping[last]['index_analyzer'] = 'snovault_index_analyzer'
    #         new_mapping[last]['search_analyzer'] = 'snovault_search_analyzer'
    #         new_mapping[last]['include_in_all'] = True
    #
    # # Automatic boost for uuid
    # if 'uuid' in mapping['properties']:
    #     mapping['properties']['uuid']['index'] = True
    #     mapping['properties']['uuid']['include_in_all'] = False
    return mapping


def create_mapping_by_type(in_type, registry):
    """
    Return a full mapping for a given doc_type of in_type
    """
    # build a schema-based hierarchical mapping for embedded view
    collection = registry[COLLECTIONS].by_item_type[in_type]
    embed_mapping = type_mapping(registry[TYPES], collection.type_info.item_type)
    # finish up the mapping
    return es_mapping(embed_mapping)


def build_index(es, in_type, mapping, dry_run, check_first):
    this_index = Index(in_type, using=es)
    if(this_index.exists() and check_first):
        # compare previous mapping and current mapping to see if we need
        # to update. if not, return to save indexing
        try:
            prev_mapping = this_index.get_mapping()[in_type]['mappings'][in_type]
        except KeyError:
            pass
        else:
            if prev_mapping == mapping:
                print("index %s already exists no need to create mapping" % (in_type))
                return
    # delete the index, ignore if it doesn't exist
    this_index.delete(ignore=404)
    this_index.settings(**index_settings())
    if dry_run:
        print(json.dumps(sorted_dict({in_type: {in_type: mapping}}), indent=4))
    else:
        this_index.create(request_timeout=30)
        try:
            this_index.put_mapping(doc_type=in_type, body={in_type: mapping})
        except:
            log.exception("Could not create mapping for the collection %s", in_type)


def snovault_cleanup(es, registry):
    """
    Simple function to delete old unused snovault index if it's present
    """
    # see if the old snovault index exists
    sno_index_name = registry.settings.get('snovault.elasticsearch.index', None)
    if sno_index_name:
        snovault_index = Index(sno_index_name, using=es)
        if snovault_index.exists():
            snovault_index.delete(ignore=404)


def run(app, collections=None, dry_run=False, check_first=True):
    registry = app.registry
    if not dry_run:
        es = app.registry[ELASTIC_SEARCH]
        snovault_cleanup(es, registry)
    if not collections:
        collections = ['meta'] + list(registry[COLLECTIONS].by_item_type.keys())
    for collection_name in collections:
        if collection_name == 'meta':
            # meta mapping just contains settings
            build_index(es, collection_name, META_MAPPING, dry_run, check_first)
        else:
            mapping = create_mapping_by_type(collection_name, registry)
            build_index(es, collection_name, mapping, dry_run, check_first)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Create Elasticsearch mapping", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--item-type', action='append', help="Item type")
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument(
        '--dry-run', action='store_true', help="Don't post to ES, just print")
    parser.add_argument('config_uri', help="path to configfile")
    parser.add_argument('--check-first', action='store_true',
                        help="check if index exists first before attempting creation")
    args = parser.parse_args()

    logging.basicConfig()
    app = get_app(args.config_uri, args.app_name)

    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger('snovault').setLevel(logging.DEBUG)

    return run(app, args.item_type, args.dry_run, args.check_first)


if __name__ == '__main__':
    main()
