Changes to embedding and indexing
=================================

EMBEDDING
=========

The 4DN DCIC team has moved away from the "embed everything" mentality that was previously in snovault. In it's place, we've allowed a precise control of what gets embedded for each object type. These are the 'embedded lists' (embedded_list property) defined for each object in the corresponding /types/ file. To provide minimal embeds without forcing us to have to manually embed common fields (read: add to individual types files), default embedding was added. This will automatically provide @id, link_id, display_title, principals_allowed, and uuid fields for any linked object at the top level. Additionally, all subobjects defined in the schema are automatically fully embedded. Embedding a linkTo with a '*' at the end of the embed path will emulate this functionality for any object in the embedded list (this means that all linkTos within that object will also get the three fields mentioned above, as well as all top level fields). Otherwise, valid embeds are single fields or just the object (in which case link_id, display_title, and uuid) are added. Below are some examples to illustrate:

In the types file
-----------------
embedded_list = [
    lab.*,
    award.title,
    submitted_by
]

Since lab, award, and submitted by are all linkTo's, default embeds will be added for each of these (this code is housed in src/fourfront_utils.py).

Starting with lab, the '*' means we add all fields for the lab as well as link_id, display_title, and uuid to any linkTo's within the lab. Let's say that lab has the following fields: title, @id, principals_allowed, link_id, display_title, and uuid. It also has the following linkTo: pi. Effectively, 'lab.*' would expand to the following given the default embedding:

[
    lab.display_title,
    lab.link_id,
    lab.uuid,
    lab.@id,
    lab.principals_allowed.*,
    lab.title,
    lab.pi.display_title,
    lab.pi.link_id,
    lab.pi.uuid,
    lab.pi.@id,
    lab.pi.principals_allowed.*
]

The next embed, award.title, is a terminal field. This means we are only interested in embedding the 'title' field for award, but we will automatically add @id, principals_allowed, link_id, display_title, and uuid to get some baseline information for the object. Thus, 'award.title' expands to:

[
    award.title,
    award.display_title,
    award.link_id,
    award.uuid,
    award.@id,
    award.principals_allowed.*
]

The last embed is 'submitted_by'. This will actually throw a warning from the tests written around fourfront_utils, since this embed is completely unnecessary. Since submitted_by is a top-level linkTo in our object, there is no reason to add it to the embedded_list; it will already get link_id, display_title, and uuid added. So let's change this embed to submitted_by.some_object (where some_object is a fictional linkTo). This would automatically add @id, principals_allowed, link_id, display_title, and uuid for some_object.

Since embedding directly affects the elasticsearch (ES) mapping and indexing, it is important to discuss the role of embedding in the mapping process. In create_mapping.py, the embedded_list for the object to be mapped is obtained and run through the fourfront_utils embedding process. These embeds are then used to create a mapping that is fitted exactly to the data that we want to embed in our object.

Similarly, the embedded fields are passed into the embed.py code to trim the result to the specific fields we desire. A cache is used during embedding to speed up the embedding process (see embed_cache.py). It caches the calculated results for view of a given item, keyed by path. For example, <my-item>/@@embedded and <my-item>/@@object would be separate entries in the cache.

INDEXING
========

As of spring 2018, the 4DN DCIC team diverged significantly from the indexing strategy previously used by ENCODE. Whereas the old system was based on creating database snapshots identified by the `xmin`, the new system queues individual items for indexing and the /index endpoint triggers a process to pull them off and index them into ES. The queues currently used are from simple queue service (SQS), an AWS product. This provided us a performance increase as well as increased visibility into what was happening in the processes (which were previously a black box). The system is built of the following components:

- Hooks to add items to the queue after a POST or PATCH (crud_views.py and invalidation.py)
- Hooks to queue items after initial mapping/subsequent remappings (create_mapping.py)
- The manager class of the queues (indexer_queue.py)
- Indexer (used for non-parallel indexing; parent of MPIndexer; indexer.py)
- MPIndexer (mpindexer.py)
- Item view to build the content to be indexed for any item (indexing_views.py)
- Index listener which drives ongoing indexing and consumption of the queue (es_index_listener.py)

Currently, the anatomy of a item to be put on the queue is:
```
{
    'uuid': <str>,
    'sid': <int or None>,
    'strict': <boolean>,
    'timestamp': <str>
}
```

uuid and timestamp are pretty self-explanatory. The sid is the DB transaction count, which is used as the version number in ES. Because sid is incremented for each transaction in the DB, this allows us a convenient method for ES versioning. The strict parameter is a boolean that controls whether or not associated uuids are also reindexed for the given uuid. In essence, if true, strict will also cause all items that embed the indexed item to also be reindexed. Currently, if an sid is provided (i.e. the item is queued through a POST or PATCH), then the embedded uuids within the item itself will also be queued.

There are currently 4 different queues for each Fourfront environment. The `primary` queue contains all items that are posted or patched. The `secondary` queue contains all items that are found as a result of finding associated uuids with a primary item (if strict == False) and also the embedded uuids of items in the primary queue. The secondary queue is also the target of items queued by create_mapping. The `deferred` queue has similar priority to `primary` and is used to contain items that cannot be updated in the scope of the currently running indexing process. They must wait for the indexer to finish and restart with a new transaction to go through. Note that if there are items in `deferred` and also `secondary`, the indexer will restart in order to index the deferred items before moving on to the secondary items. The last queue is the dead letter queue, or `dlq`. It is used for items that have failed to go through the other queues 4 times and simply holds those actions until cleared manually. The contents of the dlq are never automatically indexed again.

The process of finding the secondary uuids that need to be indexed when a primary item is created or edited is called invalidation and has it's own document (invalidation.rst). This process is complex and has been one of the largest pain points in creating and optimizing our indexing system. Please read that document for an in-depth overview of invalidation. From the indexing standpoint, all secondary items that are invalidated are indexed on the secondary queue so that they themselves do not cause a further cascading of more items on the secondary queue. Reverse links (rev_links) are taken into account during the invalidation process.

A couple endpoints were added to make the queue more useful. First, /indexing_status takes a GET request and returns the counts of items in each of the 4 queues. The /queue_indexing endpoint is a POST endpoint used to manually queue items. It requires administrator privileges and takes a JSON body where you can either specify a list of `collections` (e.g. file_fastq or biosample) or a list of `uuids` for indexing. You can also specify whether the items should be indexed in strict mode using the `strict` keyword and a boolean value. Lastly, you can specify which queue you want to send your items to using the `target_queue` keyword and a value of `primary`, `secondary`, or `deferred`. The default strict value is False and the default target is primary.

The queue is only actively cleared when create-mapping is run for a total reindex. This is because past records should not be lost for the alternate create-mapping functions, such as --check-first or --index-diff. The queue can be managed directly from the AWS console.

Please note that you must have the correct AWS credentials configured for your project to use it.

Further possible improvements to the queue system include:
- Creating a metadata layer that would allow for deduplication of <uuid/sid> combinations within the queue.
- Using an SQS FIFO queue, which could address the task above by using deduplication.
- Change the /index endpoint to only pull and index one item per transaction, which would eliminate need for the deferred queue.
- Ordering of items on the queue, either at time of create_mapping (initial indexing) or for ongoing indexing.
- Speed up indexing by refining what items are considered invalid when an item is indexed (in most cases, secondary items do not need to be reindexed).

Other related improvements:
- Use closure tables in the embedding process to make the /index-data view much faster.
