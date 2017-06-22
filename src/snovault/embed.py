from copy import deepcopy
from .cache import ManagerLRUCache
from past.builtins import basestring
from posixpath import join
from pyramid.compat import (
    native_,
    unquote_bytes_to_wsgi,
)
from pyramid.httpexceptions import HTTPNotFound
from pyramid.traversal import find_resource
from pyramid.interfaces import IRoutesMapper
import logging
log = logging.getLogger(__name__)


def includeme(config):
    config.scan(__name__)
    config.add_renderer('null_renderer', NullRenderer)
    config.add_request_method(embed, 'embed')
    config.add_request_method(lambda request: set(), '_embedded_uuids', reify=True)
    config.add_request_method(lambda request: set(), '_linked_uuids', reify=True)
    config.add_request_method(lambda request: None, '__parent__', reify=True)


def make_subrequest(request, path):
    """ Make a subrequest

    Copies request environ data for authentication.

    May be better to just pull out the resource through traversal and manually
    perform security checks.
    """
    env = request.environ.copy()
    if path and '?' in path:
        path_info, query_string = path.split('?', 1)
        path_info = path_info
    else:
        path_info = path
        query_string = ''
    env['PATH_INFO'] = path_info
    env['QUERY_STRING'] = query_string
    subreq = request.__class__(env, method='GET', content_type=None,
                               body=b'')
    subreq.remove_conditional_headers()
    # XXX "This does not remove headers like If-Match"
    subreq.__parent__ = request
    return subreq


embed_cache = ManagerLRUCache('embed_cache')

# Carl: embed is called on any request.embed (config.add_request_method(embed, 'embed'))
#       indexing_views calls embeds with a field_to_embed parameter (my change)
#       Currently, subrequests are recursively calling embed by going through
#       @@object in resource_views.py (this time, without fields_to_embed)
#       For example, if indexing a user, the order might be:
#       users/<uuid>/@@index-data --> users/<uuid>/@@embedded
#       --> users/<uuid>/@@object --> labs/<uuid>/@@object
#       --> awards/<uuid>/@@object (and so on)
#       Embedding, as it stands, is complete in its recursiveness. Objects are
#       fully embedded all the way down
#       One option would be to use fields_to_embed to limit recursion
def embed(request, *elements, **kw):
    """ as_user=True for current user
    Pass in fields_to_embed as a keyword arg
    """
    # Should really be more careful about what gets included instead.
    # Cache cut response time from ~800ms to ~420ms.
    fields_to_embed = kw.get('fields_to_embed')
    as_user = kw.get('as_user')
    path = join(*elements)
    path = unquote_bytes_to_wsgi(native_(path))
    # check to see if this embed is a non-object field
    invalid_check = identify_invalid_embed(request, path)
    if invalid_check != 'valid':
        return invalid_check
    if as_user is not None:
        result, embedded, linked = _embed(request, path, as_user)
    else:
        # Carl: caching restarts at every call to _embed. This is not the problem
        cached = embed_cache.get(path, None)
        if cached is None:
            cached = _embed(request, path)
            embed_cache[path] = cached
        result, embedded, linked = cached
        result = deepcopy(result)
    log.debug('embed: %s', path)
    request._embedded_uuids.update(embedded)
    request._linked_uuids.update(linked)
    # if we have a list of fields to embed, @@embebded is being called.
    # parse and trim fully embedded obj according to the fields to embed.
    if fields_to_embed is not None:
        p_result = parse_embedded_result(request, result, fields_to_embed)
        return p_result
    return result


def _embed(request, path, as_user='EMBED'):
    # Carl: the subrequest is 'built' here, but not actually invoked
    subreq = make_subrequest(request, path)
    subreq.override_renderer = 'null_renderer'
    if as_user is not True:
        if 'HTTP_COOKIE' in subreq.environ:
            del subreq.environ['HTTP_COOKIE']
        subreq.remote_user = as_user
    try:
        # Carl: this is key. Recursion is triggered by causing a GET @@object
        #       resource_views. result will be the @@object result, i.e. with
        #       calculated properties (@id, @type, etc.)
        #       @id strings in the result are ALL embedded (still not completely
        #       how this works). The end result is a FULLY embedded object in
        #       the top level, @@embedded, call to _embed
        result = request.invoke_subrequest(subreq)
    except HTTPNotFound:
        raise KeyError(path)
    return result, subreq._embedded_uuids, subreq._linked_uuids


def identify_invalid_embed(request, path, use_literal=False):
    """
    With new embedding system, we might attempt to embed something that doesn't
    have a fully formed path (i.e. uuid/@@object instead of /type/uuid/@@object)
    so abort embed when this path is given.
    This is okay because this only occurs when a specific field is desired;
    the object needed for that field will already be handled.
    Return the value of the non-obj field, else 'valid' if obj can be embedded

    This function is used in two ways:
    1. to differentiate from legitimate objects vs fields in the embedded
    subrequest chain (as explained above)
    2. to identify object paths in the parsing of fully embedded objects when
    given a string value (see handle_string_embed). In this case, use_literal
    should be true
    """
    split_path = path.split('/')
    invalid_return_val = None
    use_path = None
    if use_literal:
        invalid_return_val = path
        use_path = path
    else:
        # non-literal path is used, which means remove any @@ subelements.
        # Specifically, remove /@@object, which gets appended to fields as part
        # of the subrequest chain in embeddeding
        invalid_return_val = path[:-9] if path[-9:] == '/@@object' else split_path[0]
        proc_path = [sub for sub in split_path if sub[:2] != '@@']
        use_path = '/'.join(proc_path)
    if len(path) == 0 or path[0] != '/':
        return invalid_return_val
    mapper = request.registry.queryUtility(IRoutesMapper)
    if mapper.get_route(path.strip('/')):
        return 'valid'
    try:
        find_attempt = find_resource(request.root, use_path)
    except KeyError: # KeyError is due to path not found
        return invalid_return_val
    # TypeErrors come from certain formatting issues
    # Known issues: ':' in path (pyramid interprets as scheme)
    except TypeError:
        return invalid_return_val
    return 'valid' # this obj can be embedded (a valid resource path)


def parse_embedded_result(request, result, fields_to_embed):
    """
    Take a list of fields to embed, with each item being a '.' separated list of
    fields (i.e biosource.individual.organism.name). Uses these to parse and
    trim down the fully embedded result
    Returns the trimmed (selectively embedded) result
    """
    embedded_model = build_embedded_model(fields_to_embed)
    return build_embedded_result(request, result, embedded_model)


def build_embedded_model(fields_to_embed):
    """
    Takes a list of fields to embed and builds the framework used to parse
    the fully embedded result. 'fields_to_use' refer to specific fields that are to
    be embedded within an object. The base level object gets a special flag,
    '*', which means all non-object fields are embedded by default.
    Below is an example calculated from the following fields:
    INPUT:
    [modifications.modified_regions.chromosome,
    lab.uuid,
    award.*,
    biosource.*]
    OUTPUT:
    {'modifications': {'modified_regions': {'fields_to_use': ['chromosome']}},
     'lab': {'fields_to_use': ['uuid']},
     'award': {'*': ['fully embed this object']},
     'bisource': {'*': ['fully embed this object']},
     '*': ['fully embed this object']}
    """
    embedded_model = {'*':['fully embed this object']}
    for field in fields_to_embed:
        split_field = field.split('.')
        field_pointer = embedded_model
        for subfield in split_field:
            if subfield == split_field[-1]:
                if subfield == '*':
                    # '*' means all fields are used
                    field_pointer['*'] = ['fully embed this object']
                elif 'fields_to_use' in field_pointer:
                    field_pointer['fields_to_use'].append(subfield)
                else:
                    field_pointer['fields_to_use'] = [subfield]
                continue
            elif subfield not in field_pointer:
                field_pointer[subfield] = {}
            field_pointer = field_pointer[subfield]
    return embedded_model


def build_embedded_result(request, result, embedded_model):
    """
    Uses the embedded model from build_embedded_model() and uses it to recursively
    though handle_dict_embed() to build a selectively embedded result.
    Loops through the key, val pairs in the fully embedded result and checks
    against the model, adding them to the parsed_result if they are included.
    """
    parsed_result = {}
    # Use all fields, check all possible embeds
    if '*' in embedded_model:
        fields_to_use = result.keys()
    # This is true when there are no embedded objects down the line; that is,
    # we need only be concerned about fields on the current obj level.
    # Embed only the fields specified in the model when this is the case
    elif list(embedded_model) == ['fields_to_use']:
        # eliminate all fields that are not found within the actual results
        fields_to_use = [val for val in result if val in embedded_model['fields_to_use']]
    # Use any applicable fields and any embedded objects
    else:
        # find any fields on this level to use
        curr_level_fields = []
        if 'fields_to_use' in embedded_model:
            curr_level_fields = [val for val in result if val in embedded_model['fields_to_use']]
        # find fields that correspond to deeper embedded objs
        embed_objs = [val for val in embedded_model if val != 'fields_to_use']
        fields_to_use = curr_level_fields + embed_objs
    for key in fields_to_use:
        val = result.get(key)
        if not val:
            continue
        embed_val = None
        if isinstance(val, str):
            embed_val = handle_string_embed(request, key, val, embedded_model)
        elif isinstance(val, list):
            embed_val = handle_list_embed(request, key, val, embedded_model)
        elif isinstance(val, dict):
            embed_val = handle_dict_embed(request, key, val, embedded_model)
        else:  # catch any other case
            embed_val = val
        # embed_val will be false if there in the case of a non-existent value
        # In such a case, don't embed even if it's requested in the fields
        if embed_val is not None:
            parsed_result[key] = embed_val
    return parsed_result


def handle_string_embed(request, key, val, embedded_model):
    """
    Allow a string to be embedded only if it's an @id field for this object
    or a non-object related string.
    Allow @@download strings regardless of format for things like links
    """
    if key == '@id' or key == 'uuid':
        return val
    elif identify_invalid_embed(request, val, True) != 'valid':
        return val
    else:
        return None


def handle_list_embed(request, key, val, embedded_model):
    """
    Handles lists, which could be either lists of embedded objects or other
    fields. Pass them on accordingly and conglomerate the results.
    """
    list_result = []
    for item in val:
        if isinstance(item, str):
            list_result.append(handle_string_embed(request, key, item, embedded_model))
        elif isinstance(item, list):
            list_result.append(handle_list_embed(request, key, item, embedded_model))
        elif isinstance(item, dict):
            list_result.append(handle_dict_embed(request, key, item, embedded_model))
        else:  # no chance of this being an embedded object, so just use it
            list_result.append(item)
    list_result = [entry for entry in list_result if entry]  # remove Falses
    return list_result if len(list_result) > 0 else None


def handle_dict_embed(request, key, val, embedded_model):
    """
    Given an object, determine whether this object should be embedded. In
    addition, check to see if something will be embedded further down the line
    (e.g. the obj is individual and individual.organism.name will be embedded).
    Else, identify a subobject (obj defined within a schema) or a fully
    embedded object that has no deeper embedding within it.
    """
    if key in embedded_model:
        # test if itself fully embedded and deeper embedding involved
        if 'fields_to_use' in embedded_model and key in embedded_model['fields_to_use']:
            # Adding this * makes all fields get added, not just the down-the-
            # line embed
            embedded_model[key]['*'] = 'fully embed this obj'
            return build_embedded_result(request, val, embedded_model[key])
        # deeper embedding involved, but not fully embedded
        else:
            return build_embedded_result(request, val, embedded_model[key])
    elif 'uuid' not in val or key in embedded_model['fields_to_use']:
        # this is a subobject or a embedded object that has no deeper embedding
        return build_embedded_result(request, val, {'*': 'fully embed this obj'})
    else:
        return None


class NullRenderer:
    '''Sets result value directly as response.
    '''
    def __init__(self, info):
        pass

    def __call__(self, value, system):
        request = system.get('request')
        if request is None:
            return value
        request.response = value
        return None
