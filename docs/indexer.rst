Indexer Documentation:
=====================


---------------
Primary Indexer
---------------

The (snovault) system organizes simple objects stored in Postgres into 'documents' that incude, among other things, object relationships in the form of embedded objects, and audits.  These documents are then indexed by elasticsearch.  The work of *indexing*, which includes constructing the documents from Postgres objects is accomplished by a separate *master indexer process* that may then use multiple *worker processes* (via mpindexer.py) to accomplish the tasks.  Upon initialization, the master indexer process will index all documents from all uuids (object ids) in the Postgres database.  After initial indexing, the process wakes every 60 seconds and checks to see if there have been any Postgres transactions since the previous indexing.  If so, a list of uuids for all changed database objects and all related objects is generated and a new indexing cycle begins on just those uuids.  If the list of uuids is large enough (currently 100K or more), the entire set of objects is reindexed.  If there are any **followup indexers**, then the primary indexer will stage the list of uuids just indexed so those indexers may begin work.


-----------------
Followup Indexers
-----------------

Currently snovault has no followup indexers.  If it did, they would act on uuids staged by the primary indexer at the end if its cycle.  Like the primary indexer, each followup indexer is intended to run in a separate process and wakes up every N seconds to see if there is anything to do.

--------------------------
_indexer values (listener)
--------------------------

The *indexer listener* reports certain current and historical values from an *in-memory* JSON object seen via the ``/_indexer`` path. Key values are described here:

  :status: The indexer is either 'waiting' between cycles or 'indexing' during a cycle.
  :started: The time the indexer process started.  This will reflect the most recent startup, which is not necessarily the time the server was first initialized.
  :timestamp: Time of the latest cycle.
  :timeout: Number of seconds the process sleeps between cycles.
  :listening: True when Postgres is reachable and not in recovery.
  :recovery: False unless the database in recovery.
  :errors: If an error occurred while trying to run the indexer, it should appear here.  This is distinct from result errors described below.
  :max_xid: This is a Postgres transaction id which should rise with each database change.  It is used to ensure a consistent view of data during an indexing cycle.
  :snapshot: Most recent postgres snapshot identifier.  As with xid, this is used by the indexer to ensure a consistent view of data.
  :last_result: Result values from the latest cycles **whether anything was indexed or not**.
  :results: An array of up to 10 results from the most recent cycles that *actually indexed* something.

    :title: Which indexer ran. This will be 'primary_indexer' for path /_indexer.  Other idexers exist in encoded.
    :timestamp: Time of this cycle.
    :xmin: Postgres transaction id of this cycle.
    :indexed: Number of objects indexed in this cycle.
    :last_xmin: Postgres transaction id of last cycle.  Indexing should have covered all objects changed between last_xmin and xmin.
    :status: This should say 'done' as the results are displayed after a cycle has completed.  See the next section on querying the state of a current cycle.
    :cycles: Count of indexer cycles that actually indexed something. This number should reflect all cycles since the system was initialized or since a full reindexing was requested.
    :errors: If there were any errors indexing specific objects, they should appear here.
    :updated: On small indexing cycle, may contain uuids of updated objects in Postgres.
    :renamed: On small indexing cycle, may contain uuids of renamed objects in Postgres.
    :types: On small indexing cycle, may contain '\@type's of changed objects in Postgres.
    :stats: This contains the raw stats from the response header for this indexer call.

------------------
_indexer_state API
------------------

In addition to using path /_indexer, a more complete image of an indexer can be accessed via the ``/_indexer_state`` path. This require admin login to be accessed as will become clear below.

These view will return the following values:

  :title: Should be 'primary_indexer'.
  :status: The indexer is either 'waiting' between cycles or 'indexing' during a cycle.  It might also be 'uninitialized' when the system is first coming up.
  :docs_in_index: The count of all documents currently in the elasticsearch index.
  :uuids_in_progress: The count of uuids currently being indexed.
  :uuids_last_cycle: The number of uuids in the previous cycle.
  :uuids_troubled: The number of uuids that failed to index during the last cycle.
  :to_be_staged_for_follow_up_indexers: If followup indexers exist, this is the count of uuids that will be staged by the primary indexer when its current cycle completes.
  :registered_indexers: (primary only) List of indexers that have started.
  :now: The UTC time this view was displayed.  Useful for comparing to other times found here.
  :listener: The contents of an ``/_indexer`` request.  *Described above*.
  :reindex_requested: If reindexing was requested this will contain 'all' or a list of uuids.
  :notify_requested: If notify was requested, this will include who to notify and in which circumstances.
  :state: The contents of the indexer's state object held in elasticsearch...

    :title: Should be 'primary_indexer'.
    :status: The indexer is either 'done' with a cycle or 'indexing' during a cycle.
    :cycles: Count of indexer cycles that actually indexed something. This number should reflect all cycles since the system was initialized or since a full reindexing was requested.
    :cycle_count: When indexing, the number of uuids in the current cycle.
    :cycle_took: How long it took to complete the most recent indexer cycle.
    :cycle_started: When the most recent indexing cycle started.
    :indexed: Number of objects indexed in the most recent cycle.
    :indexing_elapsed: When currently indexing, this will be the amount of time since indexing started.
    :invalidated: Number of uuids needing to be indexed.
    :renamed: uuids of objects renamed in postgres.
    :updated: uuids of objects updated in postgres.
    :referencing: Count of uuids referenced by objects updated or renamed in postgres.
    :txn_count: Number of postgres transactions this cycle covers.
    :xmin: Postgres transaction id of this cycle.
    :last_xmin: Postgres transaction id of last cycle.  Indexing should have covered all objects changed between last_xmin and xmin.
    :max_xid: This is a Postgres transaction id which should rise with each database change.  It is used to ensure a consistent view of data during an indexing cycle.
    :first_txn_timestamp: Timestamp of when the postgres tranaction occurred which led to this indexing cycle.

Several requests can be made of the ``/_indexer_state`` path with use of ?request=value appended to the url:

  :uuids: Displays up to 100 uuids currently indexing starting with the uuids=Nth in the list.
  :reindex: Use 'all' for complete reindexing or comma separated uuids for specific reindexing.  This powerful method necessitates being logged on with admin permissions.
  :notify: One or more comma separated slack ids to be notified when the specific indexer is done.

    :bot_token: For the time being this is required for slack notification to work.

**Examples:**

1. | Request reindexing a single uuid (which will be expanded to related uuids). Notify Ben when indexing is done.
   | ``.../_indexer_state?reindex=4871cc67-c9c7-4f11-8628-8e9653ddb2a4&notify=hitz&bot_token=<bot_token_not_shown_here>``
2. | Request reindexing all uuids. Notify Ben when done. *NOTE: bot_token once set for this machine (previous request) is never needed again.*
   | ``.../_indexer_state?reindex=all&notify=hitz``
3. | Request up to 100 uuids currently being indexed, starting at the beginning:
   | ``.../_indexer_state?uuids=0``

