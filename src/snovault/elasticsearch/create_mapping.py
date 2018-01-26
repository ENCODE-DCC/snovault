"""\
Example.

To load the initial data:

    %(prog)s production.ini

"""
from pyramid.paster import get_app
from elasticsearch import RequestError
from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
    ConnectionTimeout
)
from elasticsearch_dsl import Index, Search
from elasticsearch_dsl.connections import connections
from functools import reduce
from snovault import (
    COLLECTIONS,
    TYPES,
)
from snovault.schema_utils import combine_schemas
from snovault.fourfront_utils import add_default_embeds, get_jsonld_types_from_collection_type
from .interfaces import ELASTIC_SEARCH
import collections
import json
import logging
import time
import datetime
import sys
from snovault.commands.es_index_data import run as run_index_data
from .indexer_utils import find_uuids_for_indexing
import transaction
import os
import argparse


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


def schema_mapping(field, schema, top_level=False):
    """
    Create the mapping for a given schema. Can handle using all fields for
    objects (*), but can handle specific fields using the field parameter.
    This allows for the mapping to match the selective embedding.
    """
    if 'linkFrom' in schema:
        type_ = 'string'
    else:
        type_ = schema['type']

    # Elasticsearch handles multiple values for a field
    if type_ == 'array' and schema['items']:
        return schema_mapping(field, schema['items'])

    if type_ == 'object':
        properties = {}
        for k, v in schema.get('properties', {}).items():
            mapping = schema_mapping(k, v)
            if mapping is not None:
                if field == '*' or k == field:
                    properties[k] = mapping
        if top_level:
            # only include include_in_all: True in top level
            return {
                'include_in_all': True,
                'properties': properties,
            }
        else:
            return {
                'properties': properties,
            }

    # hardcode fields with dates for now
    if field == 'date_created':
        return {
            'type': 'date',
            'format': "date_optional_time",
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

        if field in NON_SUBSTRING_FIELDS:
            if field in PATH_FIELDS:
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


def index_settings(in_type):
    if in_type == 'meta':
        field_limit = 1000000
    else:
        field_limit = 5000
    return {
        'index': {
            'number_of_shards': 3,
            'number_of_replicas': 1,
            'max_result_window': 100000,
            'mapping': {
                'total_fields': {
                    'limit': field_limit
                },
                'depth': {
                    'limit': 30
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


# generate an index record, which contains a mapping and settings
def build_index_record(mapping, in_type):
    return {
        'mappings': mapping,
        'settings': index_settings(in_type)
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
                        'index': True,
                        'type': 'keyword',
                    },
                },
            },
            {
                'template_unique_keys': {
                    'path_match': "unique_keys.*",
                    'mapping': {
                        'index': True,
                        'type': 'keyword',
                    },
                },
            },
            {
                'template_links': {
                    'path_match': "links.*",
                    'mapping': {
                        'index': True,
                        'type': 'keyword',
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
    mapping = schema_mapping('*', schema, True)
    embeds = add_default_embeds(item_type, types, type_info.embedded_list, schema)
    embeds.sort()
    if not embed:
        return mapping
    for prop in embeds:
        single_embed = {}
        curr_s = schema
        curr_m = mapping
        split_embed_path = prop.split('.')
        for curr_e in split_embed_path:
            # if we want to map all fields (*), do not drill into schema
            if curr_e != '*':
                # drill into the schemas. if no the embed is not found, break
                subschema = curr_s.get('properties', {}).get(curr_e, None)
                curr_s = merge_schemas(subschema, types)
            if not curr_s:
                break
            curr_m = update_mapping_by_embed(curr_m, curr_e, curr_s)
    return mapping


def merge_schemas(subschema, types):
    """
    Merge any linked schemas into the current one. Return None if none present
    """
    if not subschema:
        return None
    # handle arrays by simply jumping into them
    # we don't care that they're flattened during mapping
    ref_types = None
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
        embedded_types = [types[t].schema for t in ref_types
                          if t in types.all]
        if not embedded_types:
            return None
        curr_s = reduce(combine_schemas, embedded_types)
    return curr_s


def update_mapping_by_embed(curr_m, curr_e, curr_s):
    """
    Update the mapping based on the current mapping (curr_m), the current embed
    element (curr_e), and the processed schemas (curr_s).
    when curr_e = '*', it is a special case where all properties are added
    to the object that was previously mapped.
    """
    # see if there's already a mapping associated with this embed:
    # multiple subobjects may be embedded, so be careful here
    mapped = schema_mapping(curr_e, curr_s)
    if curr_e == '*':
        if 'properties' in mapped:
            curr_m['properties'].update(mapped['properties'])
        else:
            curr_m['properties'] = mapped
    elif curr_e in curr_m['properties'] and 'properties' in curr_m['properties'][curr_e]:
        if 'properties' in mapped:
            curr_m['properties'][curr_e]['properties'].update(mapped['properties'])
        else:
            curr_m['properties'][curr_e] = mapped
        curr_m = curr_m['properties'][curr_e]
    else:
        curr_m['properties'][curr_e] = mapped
        curr_m = curr_m['properties'][curr_e]
    return curr_m


def create_mapping_by_type(in_type, registry):
    """
    Return a full mapping for a given doc_type of in_type
    """
    # build a schema-based hierarchical mapping for embedded view
    collection = registry[COLLECTIONS].by_item_type[in_type]
    embed_mapping = type_mapping(registry[TYPES], collection.type_info.item_type)
    # finish up the mapping
    return es_mapping(embed_mapping)


def build_index(app, es, in_type, mapping, uuids_to_index, dry_run, check_first, force=False, index_diff=False, print_count_only=False):
    """
    Creates an es index for the given in_type with the given mapping and
    settings defined by item_settings(). If check_first == True, attempting
    to see if the index exists and is unchanged from the previous mapping.
    If so, do not delete and recreate the index to save on indexing.
    This function will trigger a reindexing of the in_type index if
    the old index is kept but the es doc count differs from the db doc count.
    Will also trigger a re-index for a newly created index if the indexing
    document in meta exists and has an xmin.
    """
    # determine if index already exists for this type
    this_index_record = build_index_record(mapping, in_type)
    this_index_exists = check_if_index_exists(es, in_type, check_first)
    meta_exists = check_if_index_exists(es, 'meta', check_first) if in_type != 'meta' else True

    # if the index exists, we might not need to delete it
    # if force is provided, check_first does not matter
    # otherwise, run if we are using the check-first or index_diff args
    if not force and (check_first or index_diff):
        prev_index_record = get_previous_index_record(this_index_exists, es, in_type)
        if prev_index_record is not None and this_index_record == prev_index_record:
            if in_type != 'meta':
                check_and_reindex_existing(app, es, in_type, uuids_to_index, index_diff, print_count_only)
            print('MAPPING: using existing index for collection %s' % (in_type))
            return

    if dry_run or index_diff:
        return

    # delete the index
    if this_index_exists:
        res = es_safe_execute(es.indices.delete, index=in_type, ignore=[400,404])
        if res:
            print('MAPPING: index successfully deleted for %s' % (in_type))
        else:
            print('MAPPING: could not delete index for %s' % (in_type))

    # first, create the mapping. adds settings in the body
    put_settings = this_index_record['settings']
    res = es_safe_execute(es.indices.create, index=in_type, body=put_settings, ignore=[400])
    if res:
        print('MAPPING: new index created for %s' % (in_type))
    else:
        print('MAPPING: new index failed for %s' % (in_type))

    # update with mapping
    res = es_safe_execute(es.indices.put_mapping, index=in_type, doc_type=in_type, body=mapping, ignore=[400])
    if res:
        print('MAPPING: mapping successfully added for %s' % (in_type))
    else:
        print('MAPPING: mapping failed for %s' % (in_type))

    # force means we want to forcibly re-index
    if force:
        coll_count, coll_uuids = get_collection_uuids_and_count(app, in_type)
        uuids_to_index.update(coll_uuids)
        print('MAPPING: forcibly queueing all items in the new index %s for reindexing' % (in_type))
    else:
        # if 'indexing' doc exists within meta, then re-index for this type
        indexing_xmin = None
        try:
            status = es.get(index='meta', doc_type='meta', id='indexing', ignore=[404])
        except:
            print('MAPPING: indexing record not found in meta for %s' % (in_type))
        else:
            indexing_xmin = status.get('_source', {}).get('xmin')
        if indexing_xmin is not None:
            coll_count, coll_uuids = get_collection_uuids_and_count(app, in_type)
            uuids_to_index.update(coll_uuids)
            print('MAPPING: queueing all items in the new index %s for reindexing' % (in_type))

    # put index_record in meta
    if meta_exists:
        res = es_safe_execute(es.index, index='meta', doc_type='meta', body=this_index_record, id=in_type)
        if res:
            print("MAPPING: index record created for %s" % (in_type))
        else:
            print("MAPPING: index record failed for %s" % (in_type))


def check_if_index_exists(es, in_type, check_first):
    try:
        this_index_exists = es.indices.exists(index=in_type)
    except ConnectionTimeout:
        this_index_exists = False
    if check_first and in_type == 'meta':
        for wait in [3,6,9,12]:
            if not this_index_exists:
                time.sleep(wait)
                try:
                    this_index_exists = es.indices.exists(index=in_type)
                except ConnectionTimeout:
                    this_index_exists = False
    return this_index_exists


def get_previous_index_record(this_index_exists, es, in_type):
    """
    Decide if we need to drop the index + reindex (no index/no meta record)
    OR
    compare previous mapping and current mapping + settings to see if we need
    to update. if not, use the existing mapping to prevent re-indexing.
    """
    prev_index_hit = {}
    if this_index_exists:
        try:
            prev_index_hit = es.get(index='meta', doc_type='meta', id=in_type, ignore=[404])
        except TransportError as excp:
            if excp.info.get('status') == 503:
                es.indices.refresh(index='meta')
                time.sleep(3)
                try:
                    prev_index_hit = es.get(index='meta', doc_type='meta', id=in_type, ignore=[404])
                except:
                    return None
        prev_index_record = prev_index_hit.get('_source')
        return prev_index_record
    else:
        return None


def check_and_reindex_existing(app, es, in_type, uuids_to_index, index_diff=False, print_counts=False):
    """
    lastly, check to make sure the item count for the existing
    index matches the database document count. If not, queue the uuids_to_index
    in the index for reindexing.
    If index_diff, store uuids for reindexing that
    """
    db_count, es_count, db_uuids, diff_uuids = get_db_es_counts_and_db_uuids(app, es, in_type, index_diff)
    if print_counts:
        log.warn("DB count is %s and ES count is %s for index: %s" %
                 (str(db_count), str(es_count), in_type))
        if index_diff and diff_uuids:
            log.warn("The following UUIDs are found in the DB but not the ES index: %s" % (in_type))
            for uuid in diff_uuids:
                log.warn(uuid)
        return
    if es_count is None or es_count != db_count:
        if index_diff:
            print('MAPPING: queueing %s items found in DB but not ES in the index %s for reindexing' % (str(len(diff_uuids)), in_type))
            uuids_to_index.update(diff_uuids)
        else:
            print('MAPPING: queueing all items in the existing index %s for reindexing' % (in_type))
            uuids_to_index.update(db_uuids)


def get_db_es_counts_and_db_uuids(app, es, in_type, index_diff=False):
    """
    Return the database count and elasticsearch count for a given item type,
    the list of collection uuids from the database, and the list of uuids
    found in the DB but not in the ES store.
    """
    if check_if_index_exists(es, in_type, False):
        if index_diff:
            search = Search(using=es, index=in_type, doc_type=in_type)
            search_source = search.source([])
            es_uuids = [h.meta.id for h in search_source.scan()]
            es_count = len(es_uuids)
        else:
            count_res = es.count(index=in_type, doc_type=in_type)
            es_count = count_res.get('count')
            es_uuids = []
    else:
        es_count = 0
        es_uuids = []
    # logic for datastore
    datastore = None
    try:
        datastore = app.datastore
    except AttributeError:
        pass
    else:
        if datastore == 'elasticsearch':
            app.datastore = 'database'
    db_count, db_uuids = get_collection_uuids_and_count(app, in_type)
    # reset datastore
    if datastore:
        app.datastore = datastore
    # find db uuids not in es uuids
    # maybe save time by skipping set operation if we don't need it
    if index_diff:
        diff_uuids = list(set(db_uuids) - set(es_uuids))
    else:
        diff_uuids = []
    return db_count, es_count, db_uuids, diff_uuids


def get_collection_uuids_and_count(app, in_type):
    """
    Return a count of items in a collection and a list of the uuids of those
    items. Returns only the items of the exact type specified, and not types
    that inherit from it (for example, experiment_set_replicate count will
    be subtracted from experiment_set count)
    """
    # must handle collections that have children inheriting from them
    # use specific collections and adjust if necessary
    db_count = 0
    coll_uuids = []
    non_coll_uuids = []
    check_collections = get_jsonld_types_from_collection_type(app, in_type, [in_type])
    for coll_type in check_collections:
        collection = app.registry[COLLECTIONS].get(coll_type)
        coll_count = len(collection) if collection is not None else 0
        if coll_type == in_type:
            db_count += coll_count
            coll_uuids.extend([str(uuid) for uuid in collection])
        else:
            db_count -= coll_count
            non_coll_uuids.extend([str(uuid) for uuid in collection])
    # remove uuids that aren't from the set we need
    final_uuids = [uuid for uuid in coll_uuids if uuid not in non_coll_uuids]
    return db_count, final_uuids


def es_safe_execute(function, **kwargs):
    exec_count = 0
    while exec_count < 3:
        try:
            function(**kwargs)
        except ConnectionTimeout:
            exec_count += 1
            print('ES connection issue! Retrying.', file=sys.stderr)
        else:
            return True
    return False


def snovault_cleanup(es, registry):
    """
    Simple function to delete old unused snovault index if it's present.
    Also removes any current uuid_store
    """
    # see if the old snovault index exists
    sno_index_name = registry.settings.get('snovault.elasticsearch.index', None)
    if sno_index_name:
        snovault_index = Index(sno_index_name, using=es)
        if snovault_index.exists():
            snovault_index.delete(ignore=404)
    res = es_safe_execute(es.delete, index='meta', doc_type='meta', id='uuid_store', ignore=[404])
    if res:
        print('MAPPING: removed previous UUID store from ES.')


def run_indexing(app, indexing_uuids):
    """
    indexing_uuids is a set of uuids that should be reindexed. If global args
    are available, then this will spawn a new process to run indexing with.
    Otherwise, run with the current INDEXER
    """
    run_index_data(app, uuids=indexing_uuids)


def set_es_uuid_store(es, indexing_uuids):
    """
    Takes a set of uuids to be indexed and puts them in the meta/meta/uuid_store
    for later reindexing
    """
    timestamp = datetime.datetime.now().isoformat()
    record = {}
    record['timestamp'] = timestamp
    record['uuids'] = list(indexing_uuids)
    res = es_safe_execute(es.index, index='meta', doc_type='meta', body=record, id='uuid_store')
    if res:
        print("MAPPING: uuids to index were stored succesfully.")
    else:
        print("MAPPING: uuids to index were NOT stored succesfully.")


def run(app, collections=None, dry_run=False, index_uuids=None, check_first=False, force=False, index_diff=False, strict=False, sync_index=False, no_meta=False, print_count_only=False):
    """
    Run create_mapping. Has the following options:
    collection: run create mapping for the given list of collections (indices) only.
    dry_run: if True, do not delete/create indices
    index_uuids: do not attempt to change/create mappings but force indexing of
        the given list of uuids. Will fail if the indices are not build yet.
        Can be combined with "strict" and "sync_index" parameters.
    check_first: if True, attempt to keep indices that have not changed mapping.
        If the document counts in the index and db do not match, queue
        the all items in the index for reindexing.
    force: if True, automatically recreate indices and reindex all items
        in them. Takes precedence over check_first
    index_diff: if True, do NOT create/delete indices but identify any items
        that exist in db but not in es and reindex those.
        Takes precedence over check_first and force
    print_count_only: if True, print counts for existing indices instead of
        queueing items for reindexing. Must to be used with check_first.
    strict: if True, do not include associated items when considering what
        items to reindex. Must be used with create_mapping or force.
    sync_index: if True, synchronously run reindexing instead of using uuid store.
    """
    import pdb; pdb.set_trace()
    registry = app.registry
    es = app.registry[ELASTIC_SEARCH]
    log.warn('___ES NODE INFO___:\n\n %s' % (str(es.nodes.info())))
    # keep a set of all uuids to be reindexed, which occurs after all indices
    # are created
    if index_uuids and isinstance(index_uuids, list):
        # skip create_mapping and index these uuids directly
        uuids_to_index = set(index_uuids)
    else:
        uuids_to_index = set()
        if not dry_run:
            snovault_cleanup(es, registry)
        if not collections:
            collections = list(registry[COLLECTIONS].by_item_type.keys())
        # meta could be added through item-type arg
        if not no_meta and 'meta' not in collections:
            collections = ['meta'] + collections
        for collection_name in collections:
            if collection_name == 'meta':
                # meta mapping just contains settings
                build_index(app, es, collection_name, META_MAPPING, uuids_to_index,
                            dry_run, check_first, force, index_diff, print_count_only)
            else:
                mapping = create_mapping_by_type(collection_name, registry)
                build_index(app, es, collection_name, mapping, uuids_to_index,
                            dry_run, check_first, force, index_diff, print_count_only)
    if uuids_to_index:
        # if strict option and we have an item-type(s) provided,
        # find associated uuids for indexing
        if strict:
            uuids_to_index = uuids_to_index
        else:
            uuids_to_index, _, _ = find_uuids_for_indexing(app.registry, uuids_to_index, uuids_to_index, log)
        # only index (synchronously) if --sync-index option is used
        # otherwise, store uuids for later indexing in ES uuid_store document
        if sync_index:
            print("MAPPING: indexing %s items" % (str(len(uuids_to_index))))
            run_indexing(app, uuids_to_index)
        else:
            print("MAPPING: storing %s items for reindexing..." % (str(len(uuids_to_index))))
            set_es_uuid_store(es, uuids_to_index)
    return list(uuids_to_index)


def main():
    parser = argparse.ArgumentParser(
        description="Create Elasticsearch mapping", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--item-type', action='append', help="Item type")
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument(
        '--dry-run', action='store_true', help="Don't post to ES, just print")
    parser.add_argument('config_uri', help="path to configfile")
    parser.add_argument('--index-uuids', action='append',
                        help="If this is provided, do not attempt to change any mappings but simply pass this list to uuids_to_index")
    parser.add_argument('--check-first', action='store_true',
                        help="check if index exists first before attempting creation")
    parser.add_argument('--force', action='store_true',
                        help="force new mapping and reindexing of all/given collections")
    parser.add_argument('--index-diff', action='store_true',
                        help="reindex any items in the db but not es store for all/given collections")
    parser.add_argument('--strict', action='store_true',
                        help="used with force or check_first in combination with item-type. Only index the given types (ignore associated items). Advanced users only")
    parser.add_argument('--sync-index', action='store_true',
                        help="add to cause reindexing to occur synchronously instead of using the meta uuid_store")
    parser.add_argument('--no-meta', action='store_true',
                        help="add to disregard the meta index")
    parser.add_argument('--print-count-only', action='store_true',
                        help="use with check_first to only print counts")

    args = parser.parse_args()

    logging.basicConfig()
    app = get_app(args.config_uri, args.app_name)

    # Loading app will have configured from config file. Reconfigure here:
    logging.getLogger('snovault').setLevel(logging.INFO)

    log.warn('_______CREATE-MAPPING_______\n\nRunning with: %s' % str(vars(args)))

    uuids = run(app, args.item_type, args.dry_run, args.index_uuids, args.check_first, args.force,
               args.index_diff, args.strict, args.sync_index, args.no_meta, args.print_count_only)
    return


if __name__ == '__main__':
    main()
