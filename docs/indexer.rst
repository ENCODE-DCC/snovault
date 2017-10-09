Indexer Documentation:
=====================

The (snovault) system organizes simple Postgres objects into documents that incude, among other things, object relationships in the form of embedded objects, and audits.  These documents are then indexed by elasticsearch.  The work of *indexing*, which includes building the documents from Postgres objects is accomplished by a separate *master indexer process* that may then use multiple *worker processes* (via mpindexer.py) to accomplish the work.  Upon initialization, the master indexer process will index all objects in the Postgres database.  After initial indexing, the process wakes every 60 seconds and checks to see if there have been any Postgres transactions since the previous indexing.  If so, a list of uuids for all changed database objects and all related objects is generated and a new indexing cycle begins on just those uuids.  If the list of uuids is large enough (currently more than 100K), the entire set of objects is reindexed.

---------------
_indexer values
---------------

The indexer reports certain current and historical values in an *in-memeory* JSON object seen via the /_indexer path.  Some of the key values are described here:

  :status: The indexer is either 'waiting' between cycles or 'indexing' during a cycle.
  :started: The time the indexer porcess started.  This will reflect the most recent startup, which is not necessarily the time the server was first initialized.
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
    :cycle_took: How long it took to complete this indexer cycle.
    :pass1_took: If 2-pass indexing is enabled, this is the time it took to index objects without audits.
    :pass2_took: If 2-pass indexing is enabled, this is the time it took to audit objects and add update that information in elasticsearch.
    indexed: Number of objects indexed in this cycle.
    last_xmin: Postgres transaction id of last cycle.  Indexing should have covered all objects changed between last_xmin and xmin.
    :status: This should say 'done' as the results are displayed after a cycle has completed.  See the next section on querying the state of a current cycle.
    :cycles: Count of indexer cycles that actually indexed something. This number should reflect all cycles since the system was initialized or since a full reindexing was requested.
    :errors: If there were any errors indexing specific objects, they should appear here.
    :updated: On small indexing cycle, may contain uuids of updated objects in Postgres.
    :renamed: On small indexing cycle, may contain uuids of renamed objects in Postgres.
    :types: On small indexing cycle, may contain '@type's of changed objects in Postgres.
    :stats: This contains the raw stats from the response header for this indexer call.

------------------------------
Indexer State in Elasticsearch
------------------------------

In addition to using path /_indexer, the current state of the indexer can be queried directly from elasticsearch at http://localhost:9200/snovault/meta/primary_indexer/_source

The state object contains the same values found in /_indexer results described above.  However the status may be 'indexing', in which case the values reflect the current cycle and the count of uuids being worked on will be found in 'cycle_count'.  Also, if 2-pass indexing is enabled, then 'pass1_took' will be seen as soon as that pass is complete, even though the full indexing cycle may still be in progress.

In addition to the primary_indexer state object, several other objects exist in elasticsearch to manage the indexer cycles.  All can be queried with http://localhost:9200/snovault/meta/{name}/_source

  :indexing: The master result used to pass the xmin from one cycle to the last_xmin of the next cycle.  Delete this object to request a complete reindexing. .. indexing object might to be replaced with primary_indexer state object.
  :primary_in_progress: Contains a list of uuids that are currently being indexed.
  :primary_troubled: Contains a list of uuids that failed to index in the last cycle.
  :primary_last_cycle: Contain a list of uuids that were indexed in the previous cycle.
  :primary_followup_prep_list: If a secondary indexer is enabled, this will contain the xmin of the current cycle followed by all uuids, staged for the secondary indexer once the current indexer has finished with them.
  :staged_by_primary_list: Contains all xmin/uuids that are ready to be handled by the secondary_indexer.
