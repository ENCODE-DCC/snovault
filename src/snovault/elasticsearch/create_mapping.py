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
from collections import OrderedDict



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
                    'index': False,
                    'store': True,
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


def schema_mapping(name, schema, field='*', top_level=False):
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
        if top_level:
            # only include include_in_all: True in top level
            return {
                'include_in_all': True,
                'properties': properties,
            }
        elif properties == {}:
            # needed because ES will eliminate empty properties by default
            return {
                'type': 'object',
            }
        else:
            return {
                'properties': properties,
            }

    # hardcode fields with dates for now
    if name == 'date_created':
        return {
            'type': 'date',
            'format':"yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ",
            'fields': {
                'raw': {
                    'type': 'keyword'
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

    if type_ == ["number", "string"]:
        return {
            'type': 'text',
            'fields': {
                'value': {
                    'type': 'float',
                    'ignore_malformed': True,
                },
                'raw': {
                    'type': 'keyword'
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
            'fields': {
                'raw': {
                    'type': 'keyword'
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
            'fields': {
                'raw': {
                    'type': 'keyword'
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

        if name in NON_SUBSTRING_FIELDS:
            if name in PATH_FIELDS:
                sub_mapping['analyzer'] = 'snovault_path_analyzer'
        return sub_mapping

    if type_ == 'number':
        return {
            'type': 'float',
            'fields': {
                'raw': {
                    'type': 'keyword'
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
            'fields': {
                'raw': {
                    'type': 'keyword'
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
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'detail': {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'level_name': {
            'type': 'text',
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
                'include_in_all': False,
            },
            'tid': {
                'type': 'text',
                'include_in_all': False,
            },
            'item_type': {
                'type': 'text',
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
                        'type': 'keyword'
                    },
                    'edit': {
                        'type': 'keyword'
                    },
                    'audit': {
                        'type': 'keyword'
                    }
                }
            },
            'embedded_uuids': {
                'type': 'text',
                'include_in_all': False
            },
            'linked_uuids': {
                'type': 'text',
                'include_in_all': False
            },
            'unique_keys': {
                'type': 'object'
            },
            'links': {
                'type': 'object'
            },
            'paths': {
                'type': 'text',
                'include_in_all': False
            },
            'audit': {
                'include_in_all': False,
                'properties': {
                    'ERROR': {
                        'properties': audit_mapping()
                    },
                    'NOT_COMPLIANT': {
                        'properties': audit_mapping()
                    },
                    'WARNING': {
                        'properties': audit_mapping()
                    },
                    'INTERNAL_ACTION': {
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
    # use top_level parameter here for schema_mapping
    mapping = schema_mapping(item_type, schema, '*', True)
    embeds = add_default_embeds(item_type, types, type_info.embedded, schema)
    if not embed:
        return mapping
    for prop in embeds:
        single_embed = {}
        curr_s = schema
        curr_m = mapping
        for curr_p in prop.split('.'):
            ref_types = None
            subschema = None
            ultimate_obj = False # set to true if on last level of embedding
            field = '*'
            # Check if only an object was given. Embed fully (leave field = *)
            if len(prop.split('.')) == 1:
                subschema = curr_s.get('properties', {}).get(curr_p, None)
                # if a non-obj field, return (no embedding is going on)
                if not subschema:
                    break
            else:
                subschema = curr_s.get('properties', {}).get(curr_p, None)
                field = curr_p
                if not subschema:
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
                curr_s = subschema
            else:
                curr_s = reduce(combine_schemas, (types[t].schema for t in ref_types))
            # Check if we're at the end of a hierarchy of embeds
            if len(prop.split('.')) > 1 and curr_p == prop.split('.')[-2]:
                # See if the next (last) field is an object
                # if not (which is proper), then this is the ultimate obj
                next_subschema = subschema.get('properties', {}).get(prop.split('.')[-1], None)
                # if subschema is none ()
                if not next_subschema:
                    ultimate_obj = True
                    field = prop.split('.')[-1]

            # see if there's already a mapping associated with this embed:
            # multiple subobjects may be embedded, so be careful here
            if 'properties' not in curr_m:
                continue
            if curr_p in curr_m['properties'] and 'properties' in curr_m['properties'][curr_p]:
                mapped = schema_mapping(curr_p, curr_s, field)
                if 'properties' in mapped:
                    curr_m['properties'][curr_p]['properties'].update(mapped['properties'])
                else:
                    curr_m['properties'][curr_p] = mapped
            else:
                curr_m['properties'][curr_p] = schema_mapping(curr_p, curr_s, field)

            if ultimate_obj: # this means we're at the at the end of an embed
                break
            else:
                curr_m = curr_m['properties'][curr_p]

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


def sortOD(od):
    """
    Little function to recursively sort dictionaries
    """
    res = OrderedDict()
    for k, v in sorted(od.items()):
        if isinstance(v, dict):
            res[k] = sortOD(v)
        else:
            res[k] = v
    return res


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
    check_first = True
    if(this_index.exists() and check_first):
        # compare previous mapping and current mapping to see if we need
        # to update. if not, return to save indexing
        try:
            prev_mapping = this_index.get_mapping()[in_type]['mappings'][in_type]
        except KeyError:
            pass
        else:
            if 'properties' in mapping and 'properties' in prev_mapping:
                # test to see if the index needs to be re-created based on mapping
                # this should only occur when schema-based changes affect the
                # embedded, mapping, so compare that between old and current mappings
                prev_compare =  sortOD(prev_mapping['properties']['embedded']['properties'])
                curr_compare = sortOD(mapping['properties']['embedded']['properties'])
            else:
                # properties not available for the meta index
                prev_compare = sortOD(prev_mapping)
                curr_compare = sortOD(mapping)
            if prev_compare == curr_compare:
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
