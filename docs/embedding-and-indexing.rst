Changes to embedding and indexing
=================================

The 4DN DCIC team has moved away from the "embed everything" mentality that
was previously in snovault. In it's place, we've allowed a precise control of
what gets embedded for each object type. These are the 'embedded' list defined
for each object in the corresponding /types/ file. To provide minimal embeds
without forcing us to have to manually embed common fields (read: add to
individual types files), default embedding was added. This will automatically
provide link_id, display_title, and uuid fields for any linked object at the
top level. Additionally, all subobjects defined in the schema are automatically
fully embedded. Embedding a linkTo with a '*' at the end of the embed path
will emulate this functionality for any object in the embedded list (this means
that all linkTos within that object will also get the three fields mentioned
above, as well as all top level fields). Otherwise, valid embeds are single
fields or just the object (in which case link_id, display_title, and uuid) are
added. Below are some examples to illustrate:

In the types file
-----------------
embedded = [
    lab.*,
    award.title,
    submitted_by
]

Since lab, award, and submitted by are all linkTo's, default embeds will be
added for each of these (this code is housed in src/fourfront_utils.py).

Starting with lab, the '*' means we add all fields for the lab as well as
link_id, display_title, and uuid to any linkTo's within the lab. Let's say that
lab has the following fields: title, link_id, display_title, and uuid. It also
has the following linkTo: pi. Effectively, 'lab.*' would expand to the following
given the default embedding:

[
    lab.display_title,
    lab.link_id,
    lab.uuid,
    lab.title,
    lab.pi.display_title,
    lab.pi.link_id,
    lab.pi.uuid
]

The next embed, award.title, is a terminal field. This means we are only
interested in embedding the 'title' field for award, but we will automatically
add link_id, display_title, and uuid to get some baseline information for
the object. Thus, 'award.title' expands to:

[
    award.title,
    award.display_title,
    award.link_id,
    award.uuid
]

The last embed is 'submitted_by'. This will actually throw a warning from the
tests written around fourfront_utils, since this embed is completely
unnecessary. Since submitted_by is a top-level linkTo in our object, there
is no reason to add it to the embedded list; it will already get link_id,
display_title, and uuid added. So let's change this embed to
submitted_by.some_object (where some_object is a fictional linkTo). This would
automatically add link_id, display_title, and uuid for some_object.

Since embedding directly affects the elasticsearch (ES) mapping and indexing,
it is important to discuss the role of embedding in the mapping process. In
create_mapping.py, the embedded list for the object to be mapped is obtained
and run through the fourfront_utils embedding process. These embeds are then
used to create a mapping that is fitted exactly to the data that we want to
embed in our object.

Similarly, the embedded fields are passed into the embed.py code to trim the
result to the specific fields we desire.
