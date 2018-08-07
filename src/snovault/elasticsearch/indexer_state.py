from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
)
from pyramid.view import view_config
from sqlalchemy.exc import StatementError
from snovault import (
    COLLECTIONS,
    DBSESSION,
    STORAGE
)
from snovault.storage import (
    TransactionRecord,
)
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER
)
import datetime
import logging
import pytz
import time
import copy
import json
import requests
import re

SEARCH_MAX = 99999  # OutOfMemoryError if too high

es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.ERROR)
log = logging.getLogger(__name__)

def includeme(config):
    config.add_route('_indexer_state', '/_indexer_state')
    config.scan(__name__)

class IndexerState(object):
    # Keeps track of uuids and indexer state by cycle.  Also handles handoff of uuids to followup indexer
    def __init__(self, es, index, title='primary', followups=[]):
        self.es = es
        self.index = index  # "index where indexerstate is stored"

        self.title           = title
        self.state_id        = self.title + '_indexer'       # State of the current or last cycle
        self.todo_set        = self.title + '_in_progress'   # one cycle of uuids, sent to the Secondary Indexer
        #self.failed_set      = self.title + '_failed'
        #self.done_set        = self.title + '_done'          # Trying to get all uuids from 'todo' to this set
        self.troubled_set    = self.title + '_troubled'      # uuids that failed to index in any cycle
        self.last_set        = self.title + '_last_cycle'    # uuids in the most recent finished cycle
        self.success_set     = None                          # None is the same as self.done_set
        self.cleanup_this_cycle = [self.todo_set]  # ,self.failed_set,self.done_set]  # Clean up at end of current cycle
        self.cleanup_last_cycle = [self.last_set,self.troubled_set]              # Clean up at beginning of next cycle
        self.override           = 'reindex_' + self.title      # If exists then reindex all
        # DO NOT INHERIT! These keys are for passing on to other indexers
        self.followup_prep_list      = 'primary_followup_prep_list' # Setting up the uuids to be handled by a followup process
        self.staged_for_vis_list     = 'staged_for_vis_indexer' # Followup list is added to here to pass baton
        self.staged_for_regions_list = 'staged_for_region_indexer'     # Followup list is added to here to pass baton
        self.followup_lists = []                                     # filled dynamically
        for name in followups:
            if name != '':
                list_id = 'staged_for_' + name
                assert list_id == self.staged_for_vis_list or list_id == self.staged_for_regions_list
                self.followup_lists.append(list_id)
        self.clock = {}
        # some goals:
        # 1) Detect and recover from interrupted cycle - working but ignored for now
        # 2) Record (double?) failures and consider blacklisting them - not tried, could do.

    # Private-ish primitives...
    def get_obj(self, id, doc_type='meta'):
        try:
            return self.es.get(index=self.index, doc_type=doc_type, id=id).get('_source',{})  # TODO: snovault/meta
        except:
            return {}

    def put_obj(self, id, obj, doc_type='meta'):
        try:
            self.es.index(index=self.index, doc_type=doc_type, id=id, body=obj)
        except:
            log.warn("Failed to save to es: " + id, exc_info=True)

    def delete_objs(self, ids, doc_type='meta'):
        for id in ids:
            try:
                self.es.delete(index=self.index, doc_type=doc_type, id=id)
            except:
                pass

    def get_list(self, id):
        return self.get_obj(id).get('list',[])

    def get_count(self, id):
        return self.get_obj(id).get('count',0)

    def put_list(self, id, a_list):
        return self.put_obj(id, { 'list': a_list, 'count': len(a_list) })

    #def get_diff(self,orig_id, subtract_ids):
    #    result_set = set(self.get_list(orig_id))
    #
    #    if len(result_set) > 0:
    #        for id in subtract_ids:
    #            subtract_list = self.get_list(id)
    #            if len(subtract_list):
    #                result_set = result_set.difference(set(subtract_list))
    #    return result_set

    def set_add(self, id, vals):
        set_to_update = set(self.get_list(id))
        if len(set_to_update) > 0:
            set_to_update.update(vals)
        else:
            set_to_update = set(vals)
        self.put_list(id, set_to_update)

    def list_extend(self, id, vals):
        list_to_extend = self.get_list(id)
        if len(list_to_extend) > 0:
            list_to_extend.extend(vals)  # TODO: consider capping at SEARCH_MAX (keeping count but not uuids).  Requires followup handoff work.
        else:
            list_to_extend = vals

        self.put_list(id, list_to_extend)

    def rename_objs(self, from_id, to_id):
        val = self.get_list(from_id)
        if val:
            self.put_list(to_id, val)
            self.delete_objs([from_id])

    # Public access...
    def get(self):
        '''Returns the basic state info'''
        return self.get_obj(self.state_id)

    def put(self, state):
        '''Update the basic state info'''
        # Don't save errors in es
        errors = state.pop('errors', None)

        state['title'] = self.state_id
        self.put_obj(self.state_id, state)

        if errors is not None:
            state['errors'] = errors

    def request_reindex(self,requested):
        '''Requests full reindexing on next cycle'''
        if requested == 'all':
            if self.title == 'primary':  # If primary indexer delete the original master obj
                self.delete_objs(["indexing"])  # http://localhost:9201/snovault/meta/indexing
            else:
                self.put_obj(self.override, {self.title : 'reindex', 'all_uuids': True})

        else:
            uuid_list = requested.split(',')
            uuids = set()
            while uuid_list:
                uuid = uuid_list.pop(0)
                if len(uuid) > 0 and re.match("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", uuid):
                    uuids.add(uuid)
                else:
                    return "Requesting reindex of at least one uninterpretable uuid: '%s'" % (uuid)
            override_obj = self.get_obj(self.override)
            if 'uuids' not in override_obj.keys():
                override_obj['uuids'] = list(uuids)
            else:
                uuids |= set(override_obj['uuids'])
                override_obj['uuids'] = list(uuids)
            self.put_obj(self.override, override_obj)
        return None

    def all_indexable_uuids(self, request):
        '''returns list of uuids pertinant to this indexer.'''
        return list(all_uuids(request.registry))

    def reindex_requested(self, request):
        '''returns list of uuids if a reindex was requested.'''
        override = self.get_obj(self.override)
        if override:
            if override.get('all_uuids', False):
                self.delete_objs([self.override] + self.followup_lists)
                return self.all_indexable_uuids(request)
            else:
                uuids =  override.get('uuids',[])
                uuid_count = len(uuids)
                if uuid_count > 0:
                    if uuid_count > SEARCH_MAX:
                        self.delete_objs([self.override] + self.followup_lists)
                    else:
                        self.delete_objs([self.override])
                    return uuids
        return None


    def get_initial_state(self):
        '''Useful to initialize at idle cycle'''
        new_state = { 'title': self.state_id, 'status': 'idle'}
        state = self.get()
        for var in ['cycles']:  # could expand this list
            val = state.pop(var,None)
            if val is not None:
                new_state[var] = val
        # Make sure indexer is registered:
        self.set_add("registered_indexers", [self.state_id])
        return new_state

    def start_clock(self, name):
        '''Can start a named clock and use it later to figure out elapsed time'''
        self.clock[name] = datetime.datetime.now(pytz.utc)

    def elapsed(self, name):
        '''Returns string of time elapsed since named clock started.'''
        start = self.clock.get(name)
        if start is None:
            return 'unknown'
        else:
            return str(datetime.datetime.now(pytz.utc) - start)

    def priority_cycle(self, request):
        '''Initial startup, reindex, or interupted prior cycle can all lead to a priority cycle.
           returns (discovered xmin, uuids, whether previous cycle was interupted).'''
        # Not yet started?
        initialized = self.get_obj("indexing")  # http://localhost:9201/snovault/meta/indexing
        if not initialized:
            self.delete_objs([self.override] + self.followup_lists)
            state = self.get()
            state['status'] = 'uninitialized'
            self.put(state)
            return (-1, [], False)  # primary indexer will know what to do and secondary indexer should do nothing yet

        state = self.get()

        # Rare call for reindexing...
        reindex_uuids = self.reindex_requested(request)
        if reindex_uuids is not None and reindex_uuids != []:
            uuids_count = len(reindex_uuids)
            log.warn('%s reindex of %d uuids requested' % (self.state_id, uuids_count))
            return (-1, reindex_uuids, False)

        if state.get('status', '') != 'indexing':
            return (-1, [], False)

        xmin = state.get('xmin', -1)
        #snapshot = state.get('snapshot', None)
        if xmin == -1:  # or snapshot is None:
            return (-1, [], False)

        #assert(self.get_count(self.done_set) == 0)  # Valid for cycle-level accounting only
        #undone_uuids = self.get_diff(self.todo_set, [self.done_set])  # works for any accountingu
        undone_uuids = self.get_list(self.todo_set)                    # works fastest for cycle-level accounting
        if len(undone_uuids) <= 0:  # TODO SEARCH_MAX?  SEARCH_MAX/10
            return (-1, [], False)

        # Note: do not clean up last cycle yet because we could be restarted multiple times.
        return (xmin, undone_uuids, True)


    def prep_for_followup(self, xmin, uuids):
        '''Prepare a cycle of uuids for passing to a followup indexer (e.g. audits, viscache)'''
        prep_list = [ "xmin:%s" % xmin ]
        prep_list.extend(uuids)
        self.put_list(self.followup_prep_list, prep_list)
        # No need to preserve anything on the prep_list as it passes to the staged list in one cycle.

    def start_cycle(self, uuids, state=None):
        '''Every indexing cycle must be properly opened.'''
        self.clock = {}
        self.start_clock('cycle')
        if state is None:
            state = self.get()
        state['cycle_started'] = datetime.datetime.now().isoformat()
        state['status'] = 'indexing'
        state['cycle_count'] = len(uuids)

        self.put(state)
        self.delete_objs(self.cleanup_last_cycle)
        self.delete_objs(self.cleanup_this_cycle)
        self.put_list(self.todo_set, set(uuids))
        return state

    def add_errors(self, errors, finished=True):
        '''To avoid 16 worker concurency issues, errors are recorded at the end of a cycle.'''
        uuids = [err['uuid'] for err in errors]  # better be uuids!
        if len(uuids) > 0:
            # Forget extend, this is cycle-level accounting
            #self.list_extend(self.failed_set, uuids)
            # Forget failed_set, this is cycle-level accounting
            #self.put_list(self, self.failed_set, uuids)

            if finished:
                # Forget sets... cycle-level accounting so errors => failed_set => troubled_set all in one cycle.
                # handle troubled uuids:
                #troubled_uuids = set(self.get_list(self.failed_set))
                #if len(troubled_uuids):
                #    self.set_add(self.troubled_set, troubled_uuids)
                #    # TODO: could make doubled_troubled set and use it to blacklist uuids
                self.put_list(self.troubled_set, uuids)

    def finish_cycle(self, state, errors=None):
        '''Every indexing cycle must be properly closed.'''

        if errors:  # By handling here, we avoid overhead and concurrency issues of uuid-level accounting
            self.add_errors(errors)

        # pass any staged items to followup
        if self.followup_prep_list is not None:
            # TODO: send signal for 'all' when appropriate.  Saves the following expensive lines.
            hand_off_list = self.get_list(self.followup_prep_list)
            if len(hand_off_list) > 0:  # Have to push because ready_list may still have previous cycles in it
                for id in self.followup_lists:
                    self.list_extend(id, hand_off_list)
                    #log.warn("prmary added to %s" % id)
                self.delete_objs([self.followup_prep_list])

        # cycle-level accounting so todo => done => last in this function
        #self.rename_objs(self.todo_set, self.done_set)
        done_count = self.get_count(self.todo_set)
        self.rename_objs(self.todo_set, self.last_set)

        if self.success_set is not None:
            state[self.title + '_updated'] = self.get_count(self.success_set)
        cycle_count = state.pop('cycle_count', None)
        #assert(cycle_count == done_count)
        state['indexed'] = done_count

        #self.rename_objs(self.done_set, self.last_set)   # cycle-level accounting so todo => done => last in this function
        self.delete_objs(self.cleanup_this_cycle)
        state['status'] = 'done'
        state['cycles'] = state.get('cycles', 0) + 1
        state['cycle_took'] = self.elapsed('cycle')

        self.put(state)

        return state

    def get_notice_user(self, user, bot_token):
        '''Returns the user token for a named user.'''
        slack_users = self.get_obj('slack_users',{})
        if not slack_users:
            try:
                r = requests.get('https://slack.com/api/users.list?token=%s' % (bot_token))
                resp = json.loads(r.text)
                if not resp['ok']:  # and resp.get('error','') != 'ratelimited':
                    log.warn(resp)  # too many at once: {'error': 'ratelimited', 'ok': False}
                    return None
                members = resp.get('members',[])
                for member in members:
                    slack_users[member['name']] = member['id']
                self.put_obj('slack_users',slack_users)
            except:
                return None
        return slack_users.get(user)

    def set_notices(self, from_host, who=None, bot_token=None, which=None):
        '''Set up notification so that slack bot can send a message when indexer finishes'''
        if who is None and bot_token is None:
            return "ERROR: must specify who to notify or bot_token"
        if which is None:
            which = self.state_id
        if which == 'all':
            which = 'all_indexers' # needed because of elasticsearch conflicts with 'all'
        elif which not in self.get_list("registered_indexers"):
            if which + '_indexer' in self.get_list("registered_indexers"):
                which += '_indexer'
            else:
                return "ERROR: unknown indexer to monitor: %s" % (which)

        notify = self.get_obj('notify', 'default')
        if bot_token is not None:
            notify['bot_token'] = bot_token

        if 'from' not in notify:
            notify['from'] = from_host

        user_warns = ''
        if who is not None:
            if who in ['none','noone','nobody','stop','']:
                notify.pop(which, None)
            else:
                indexer_notices = notify.get(which,{})
                if which == 'all_indexers':  # each indexer will have to finish
                    if 'indexers' not in indexer_notices:
                        indexer_notices['indexers'] = self.get_list("registered_indexers")
                users = who.split(',')
                if 'bot_token' in notify:
                    for name in users:
                        if self.get_notice_user(name,notify['bot_token']) is None:
                            user_warns += ', ' + name
                who_all = indexer_notices.get('who',[])
                who_all.extend(users)
                indexer_notices['who'] = list(set(who_all))
                notify[which] = indexer_notices
                # either self.state_id: {who: [...]} or 'all_indexers': {'indexers': [...], 'who': [...]}

        self.put_obj('notify', notify, 'default')
        if user_warns != '':
            user_warns = 'Unknown users: ' + user_warns[2:]
        if 'bot_token' not in notify:
            return "WARNING: bot_token is required. " + user_warns
        elif user_warns != '':
            return "WARNING: " + user_warns

        return None

    def get_notices(self, full=False):
        '''Get the notifications'''
        notify = self.get_obj('notify','default')
        if full:
            return notify
        notify.pop('bot_token', None)
        notify.pop('from', None)
        for which in notify.keys():
            if len(notify[which].get('who',[])) == 1:
                notify[which] = notify[which]['who'][0]
        indexers = self.get_list("registered_indexers")
        if self.title == 'primary':  # Primary will show all notices
            indexers.append('all_indexers')
            for indexer in indexers:
                if notify.get(indexer,{}):
                    return notify  # return if anything
        else:  # non-primary will show specific notice and all
            indexers.remove(self.state_id)
            for indexer in indexers:
                notify.pop(indexer,None)
            for indexer in [self.state_id, 'all_indexers']:
                if notify.get(indexer,{}):
                    return notify  # return if anything
        return {}

    def send_notices(self):
        '''Sends notifications when indexer is done.'''
        # https://slack.com/api/chat.postMessage?token=xoxb-1974789...&channel=U1KPQK1HN&text=Yay!
        notify = self.get_obj('notify','default')
        if not notify:
            return
        if 'bot_token' not in notify or 'from' not in notify:
            return  # silent failure, but leaves notify unchanged for evidence

        changed = False
        text = None
        who = []
        if 'all_indexers' in notify:  # 'all_indexers': {'indexers': [...], 'who': [...]}
            # if all indexers are done, then report
            indexers = notify['all_indexers'].get('indexers',[])
            if self.state_id in indexers:
                # Primary must finish before follow up indexers can remove themselves from list
                if self.state_id == 'primary_indexer' or 'primary_indexer' not in indexers:
                    indexers.remove(self.state_id)
                    if len(indexers) > 0:
                        notify['all_indexers']['indexers'] = indexers
                        changed = True
            if len(indexers) == 0:
                who.extend(notify['all_indexers'].get('who',[]))
                notify.pop('all_indexers',None)
                changed = True
                text='All indexers are done for %s/_indexer_state' % (notify['from'])
        if self.state_id in notify:  # self.state_id: {who: [...]}
            who.extend(notify[self.state_id].get('who',[]))
            notify.pop(self.state_id, None)
            changed = True
            if text is None:
                text='%s is done for %s' % (self.state_id, notify['from'])
                if self.title == 'primary':
                    text += '/_indexer_state'
                else:
                    text += '/_%sindexer_state' % (self.title)
        if len(who) > 0 and text is not None:
            who = list(set(who))
            users = ''
            msg = ''
            if len(who) == 1:
                #channel='U1KPQK1HN'  # TODO: look up user token
                channel = self.get_notice_user(who[0],notify['bot_token'])
                if channel:
                    msg = 'token=%s&channel=%s&text=%s' % (notify['bot_token'],channel,text)
                # otherwise fall back on generic message.
            if msg == '':  # This will catch multiple users AND single users for which a channel could not be found
                channel='dcc-private'
                for user in who:
                    users += '@' + user + ', '
                msg = 'token=%s&channel=%s&link_names=true&text=%s%s' % (notify['bot_token'],channel,users,text)

            try:
                r = requests.get('https://slack.com/api/chat.postMessage?' + msg)
                resp = json.loads(r.text)
                if not resp['ok']:
                    log.warn(resp)
            except:
                log.warn("Failed to notify via slack: [%s]" % (msg))

        if changed:  # alter notify even if error, so the same error doesn't flood log.
            self.put_obj('notify', notify, 'default')

    def display(self, uuids=None):
        display = {}
        display['state'] = self.get()
        if display['state'].get('status','') == 'indexing' and 'cycle_started' in display['state']:
            started = datetime.datetime.strptime(display['state']['cycle_started'],'%Y-%m-%dT%H:%M:%S.%f')
            display['state']['indexing_elapsed'] = str(datetime.datetime.now() - started)
        display['title'] = display['state'].get('title',self.state_id)
        display['uuids_in_progress'] = self.get_count(self.todo_set)
        display['uuids_troubled'] = self.get_count(self.troubled_set)
        display['uuids_last_cycle'] = self.get_count(self.last_set)
        if self.followup_prep_list is not None:
            display['to_be_staged_for_follow_up_indexers'] = self.get_count(self.followup_prep_list)
        if self.title == 'primary':
            for id in self.followup_lists:
                display[id] = self.get_count(id)
        else:
            id = 'staged_for_%s_list' % (self.title)
            display['staged_by_primary'] = self.get_count(id)

        reindex = self.get_obj(self.override)
        if reindex:
            uuids = reindex.get('uuids')
            if uuids is not None:
                display['reindex_requested'] = uuids
            elif reindex.get('all_uuids',False):
                display['reindex_requested'] = 'all'
        notify = self.get_notices()
        if notify:
            display['notify_requested'] = notify
        display['now'] = datetime.datetime.now().isoformat()

        if uuids is not None:
            uuids_to_show = []
            uuid_list = self.get_obj(self.todo_set)
            if not uuid_list:
                uuids_to_show = 'No uuids indexing'
            else:
                uuid_start = 0
                try:
                    uuid_start = int(uuids)
                except:
                    pass
                if uuid_start < uuid_list.get('count',0):
                    uuid_end = uuid_start+100
                    if uuid_start > 0:
                        uuids_to_show.append("... skipped first %d uuids" % (uuid_start))
                    uuids_to_show.extend(uuid_list['list'][uuid_start:uuid_end])
                    if uuid_list.get('count',0) > uuid_end:
                        uuids_to_show.append("another %d uuids..." % (uuid_list.get('count',0) - uuid_end))
                elif uuid_start > 0:
                    uuids_to_show.append("skipped past all %d uuids" % (uuid_list.get('count',0)))
                else:
                    uuids_to_show = 'No uuids indexing'
            display['uuids_in_progress'] = uuids_to_show

        return display

@view_config(route_name='_indexer_state', request_method='GET', permission="index")
def indexer_state_show(request):
    es = request.registry[ELASTIC_SEARCH]
    INDEX = request.registry.settings['snovault.elasticsearch.index']
    state = IndexerState(es,INDEX)

    # requesting reindex
    reindex = request.params.get("reindex")
    if reindex is not None:
        msg = state.request_reindex(reindex)
        if msg is not None:
            return msg

    # Requested notification
    who = request.params.get("notify")
    bot_token = request.params.get("bot_token")
    if who is not None or bot_token is not None:
        notices = state.set_notices(request.host_url, who, bot_token, request.params.get("which"))
        if notices is not None:
            return notices

    display = state.display(uuids=request.params.get("uuids"))
    # getting count is complicated in es5 as docs are separate indexes
    item_types = all_types(request.registry)
    count = 0
    for item_type in item_types:
        # TODO: index list should be replaced by index alias:
        #       https://www.elastic.co/guide/en/elasticsearch/reference/6.2/indices-aliases.html
        try:
            type_count = es.count(index=item_type).get('count',0)
            count += type_count
        except:
            pass
    if count:
        display['docs_in_index'] = count
    else:
        display['docs_in_index'] = 'Not Found'

    if not request.registry.settings.get('testing',False):  # NOTE: _indexer not working on local instances
        try:
            r = requests.get(request.host_url + '/_indexer')
            display['listener'] = json.loads(r.text)
            display['status'] = display['listener']['status']
        except:
            log.error('Error getting /_indexer', exc_info=True)

    display['registered_indexers'] = state.get_list('registered_indexers')
    # always return raw json
    request.query_string = "format=json"
    return display


def all_types(registry):
    collections = registry[COLLECTIONS]
    return sorted(collections.by_item_type)


def all_uuids(registry, types=None):
    # First index user and access_key so people can log in
    collections = registry[COLLECTIONS]
    initial = ['user', 'access_key']
    for collection_name in initial:
        collection = collections.by_item_type.get(collection_name, [])
        # for snovault test application, there are no users or keys
        if types is not None and collection_name not in types:
            continue
        for uuid in collection:
            yield str(uuid)
    for collection_name in sorted(collections.by_item_type):
        if collection_name in initial:
            continue
        if types is not None and collection_name not in types:
            continue
        collection = collections.by_item_type[collection_name]
        for uuid in collection:
            yield str(uuid)
