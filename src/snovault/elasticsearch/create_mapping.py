"""\
Example.

To load the initial data:

    %(prog)s production.ini

"""
from pyramid.paster import get_app
from functools import reduce
from snovault import (
    COLLECTIONS,
    TYPES,
)
from snovault.schema_utils import combine_schemas
from .interfaces import (
    ELASTIC_SEARCH,
    RESOURCES_INDEX,
)
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
        'analyzer': 'snovault_search_analyzer'
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
NON_SUBSTRING_FIELDS = ['uuid', '@id', 'submitted_by', 'md5sum',
                        'references', 'submitted_file_name']
KEYWORD_FIELDS = ['schema_version', 'uuid', 'accession', 'alternate_accessions',
                  'aliases', 'status', 'date_created', 'submitted_by',
                  'internal_status', 'target', 'biosample_type']
TEXT_FIELDS = ['pipeline_error_detail', 'description', 'notes']


def sorted_pairs_hook(pairs):
    return collections.OrderedDict(sorted(pairs))


def sorted_dict(d):
    return json.loads(json.dumps(d), object_pairs_hook=sorted_pairs_hook)


def schema_mapping(name, schema):
    # If a mapping is explicitly defined, use it
    if 'mapping' in schema:
        return schema['mapping']

    if 'linkFrom' in schema:
        type_ = 'string'
    # elif 'linkTo' in schema:
    #     type_ = 'object'
    else:
        type_ = schema['type']

    # Elasticsearch handles multiple values for a field
    if type_ == 'array':
        return schema_mapping(name, schema['items'])

    if type_ == 'object':
        properties = {}
        for k, v in schema.get('properties', {}).items():
            mapping = schema_mapping(k, v)
            if mapping is not None:
                properties[k] = mapping
        return {
            'type': 'object',
            'include_in_all': False,
            'properties': properties,
        }

    if type_ == ["number", "string"]:
        return {
            'type': 'keyword',
            'copy_to': [],
            'fields': {
                'value': {
                    'type': 'float',
                    'ignore_malformed': True,
                },
                'raw': {
                    'type': 'keyword',
                }
            }
        }

    if type_ == 'boolean':
        return {
            'type': 'boolean',
            'store': True,
            'fields': {
                'raw': {
                    'type': 'keyword',
                }
            }
        }

    if type_ == 'string':

        if name in KEYWORD_FIELDS:
            field_type = 'keyword'
        elif name in TEXT_FIELDS:
            field_type = 'text'
        else:
            field_type = 'keyword'

        sub_mapping = {
            'type': field_type
        }

        # these fields are unintentially partially matching some small search
        # keywords because fields are analyzed by nGram analyzer
        if name in NON_SUBSTRING_FIELDS:
            sub_mapping['include_in_all'] = False
        return sub_mapping

    if type_ == 'number':
        return {
            'type': 'float',
            'store': True,
            'fields': {
                'raw': {
                    'type': 'keyword',
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
                }
            }
        }


def index_settings():
    return {
        'settings': {
            'index.max_result_window': 99999,
            'index.mapping.total_fields.limit': 5000,
            'index.number_of_shards': 5,
            'index.number_of_replicas': 2,
            'analysis': {
                'filter': {
                    'substring': {
                        'type': 'nGram',
                        'min_gram': 1,
                        'max_gram': 33
                    },
                    'english_stop': {
                        'type': 'stop',
                        'stopwords': '_english_'
                    },
                    'english_stemmer': {
                        'type': 'stemmer',
                        'language': 'english'
                    },
                    'english_possessive_stemmer': {
                        'type': 'stemmer',
                        'language': 'possessive_english'
                    },
                    'delimiter': {
                        'type': 'word_delimiter',
                        'catenate_all': True,
                        'preserve_original': True,
                        'stem_english_possessive': True,
                        'split_on_numerics': False
                    }
                },
                'analyzer': {
                    'default': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'char_filter': 'html_strip',
                        'filter': [
                            'english_possessive_stemmer',
                            'lowercase',
                            'english_stop',
                            'english_stemmer',
                            'asciifolding',
                            'delimiter'
                        ]
                    },
                    'snovault_index_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'char_filter': 'html_strip',
                        'filter': [
                            'english_possessive_stemmer',
                            'lowercase',
                            'english_stop',
                            'english_stemmer',
                            'asciifolding',
                            'delimiter',
                            'substring'
                        ]
                    },
                    'snovault_search_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'filter': [
                            'english_possessive_stemmer',
                            'lowercase',
                            'english_stop',
                            'english_stemmer',
                            'asciifolding',
                            'delimiter'
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
            'type': 'keyword',
        },
        'detail': {
            'type': 'text',
            'index': 'true',
        },
        'level_name': {
            'type': 'keyword',
        },
        'level': {
            'type': 'integer',
        }
    }


def es_mapping(mapping):
    return {
        '_all': {
            'enabled': True,
            'analyzer': 'snovault_search_analyzer'
        },
        'dynamic_templates': [
            {
                'template_principals_allowed': {
                    'path_match': "principals_allowed.*",
                    'match_mapping_type': "string",
                    'mapping': {
                        'type': 'keyword',
                    },
                },
            },
            {
                'template_unique_keys': {
                    'path_match': "unique_keys.*",
                    'match_mapping_type': "string",
                    'mapping': {
                        'type': 'keyword',
                    },
                },
            },
            {
                'template_links': {
                    'path_match': "links.*",
                    'match_mapping_type': "string",
                    'mapping': {
                        'type': 'keyword',
                    },
                },
            },
            {
                'strings': {
                    'match_mapping_type': "string",
                    'mapping': {
                        'type': 'keyword',
                    },
                },
            },
            {
                'integers': {
                    'match_mapping_type': "long",
                    'mapping': {
                        'type': 'long',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                },
            }
        ],
        'properties': {
            'uuid': {
                'type': 'keyword',
                'include_in_all': False,
            },
            'tid': {
                'type': 'keyword',
                'include_in_all': False,
            },
            'item_type': {
                'type': 'keyword',
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
            'embedded_uuids': {
                'type': 'keyword',
                'include_in_all': False,
            },
            'linked_uuids': {
                'type': 'keyword',
                'include_in_all': False,
            },
            'paths': {
                'type': 'keyword',
                'include_in_all': False,
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


def combined_mapping(types, *item_types):
    combined = {
        'type': 'object',
        'properties': {},
    }
    for item_type in item_types:
        schema = types[item_type].schema
        mapping = schema_mapping(item_type, schema)
        for k, v in mapping['properties'].items():
            if k in combined:
                assert v == combined[k]
            else:
                combined[k] = v

    return combined


def type_mapping(types, item_type, embed=True):
    type_info = types[item_type]
    schema = type_info.schema
    mapping = schema_mapping(item_type, schema)
    if not embed:
        return mapping
    for prop in type_info.embedded_paths:
        s = schema
        m = mapping

        for p in prop.split('.'):
            ref_types = None

            subschema = s.get('properties', {}).get(p)
            if subschema is None:
                msg = 'Unable to find schema for %r embedding %r in %r' % (p, prop, item_type)
                raise ValueError(msg)

            subschema = subschema.get('items', subschema)
            if 'linkFrom' in subschema:
                _ref_type, _ = subschema['linkFrom'].split('.', 1)
                ref_types = [_ref_type]
            elif 'linkTo' in subschema:
                ref_types = subschema['linkTo']
                if not isinstance(ref_types, list):
                    ref_types = [ref_types]

            if ref_types is None:
                m = m['properties'][p]
                s = subschema
                continue

            s = reduce(combine_schemas, (types[t].schema for t in ref_types))

            # Check if mapping for property is already an object
            # multiple subobjects may be embedded, so be carful here
            if m['properties'][p]['type'] in ['keyword', 'text']:
                m['properties'][p] = schema_mapping(p, s)

            m = m['properties'][p]

    boost_values = schema.get('boost_values', None)
    if boost_values is None:
        boost_values = {
            prop_name: 1.0
            for prop_name in ['@id', 'title']
            if prop_name in mapping['properties']
        }
    for name, boost in boost_values.items():
        props = name.split('.')
        last = props.pop()
        new_mapping = mapping['properties']
        for prop in props:
            new_mapping = new_mapping[prop]['properties']
        new_mapping[last]['boost'] = boost
        if last in NON_SUBSTRING_FIELDS:
            new_mapping[last]['include_in_all'] = False
        else:
            new_mapping[last]['include_in_all'] = True
    return mapping


def create_elasticsearch_index(es, index, body):
    es.indices.create(index=index, body=body, wait_for_active_shards=1, ignore=[400, 404], master_timeout='5m', request_timeout=300)


def set_index_mapping(es, index, doc_type, mapping):
    es.indices.put_mapping(index=index, doc_type=doc_type, body=mapping, ignore=[400], request_timeout=300)


def create_snovault_index_alias(es, indices):
    es.indices.put_alias(index=','.join(indices), name=RESOURCES_INDEX, request_timeout=300)


def run(app, collections=None, dry_run=False):
    index = app.registry.settings['snovault.elasticsearch.index']
    registry = app.registry
    if not dry_run:
        es = app.registry[ELASTIC_SEARCH]
        print('CREATE MAPPING RUNNING')

    if not collections:
        collections = ['meta'] + list(registry[COLLECTIONS].by_item_type.keys())

    indices = []
    for collection_name in collections:
        if collection_name == 'meta':
            doc_type = 'meta'
            mapping = META_MAPPING
        else:
            index = doc_type = collection_name
            collection = registry[COLLECTIONS].by_item_type[collection_name]
            mapping = es_mapping(type_mapping(registry[TYPES], collection.type_info.item_type))

        if mapping is None:
            continue  # Testing collections
        if dry_run:
            print(json.dumps(sorted_dict({index: {doc_type: mapping}}), indent=4))
            continue
        create_elasticsearch_index(es, index, index_settings())
        set_index_mapping(es, index, doc_type, {doc_type: mapping})
        if collection_name != 'meta':
            indices.append(index)

    create_snovault_index_alias(es, indices)


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
    args = parser.parse_args()

    logging.basicConfig()
    app = get_app(args.config_uri, args.app_name)

    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger('snovault').setLevel(logging.DEBUG)

    return run(app, args.item_type, args.dry_run)


if __name__ == '__main__':
    main()
