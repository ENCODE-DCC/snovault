Changes to embedding and indexing
=================================

Changes to elasticsearch mapping and indexing were made to allow for the upgrade to ES version 2.x. The two issues encountered in snovault preventing the upgrade were:

* conflicting field types when fields were embedded in some object types but not others.

For example, if you have a USER object that has a linked object LAB embedded in it and another EXPERIMENT object with has a string reference to a LAB object that is not embedded, the LAB field between two these two objects would conflict.
To fix this, selective embedding was implemented. The basis of this is the list of embedded fields for each object TYPE file (see /src/snowflakes/types/snow.py). For example, the snowset type has an embedded list of [snowflakes, snowflakes.lab, and so on...].
As part of the indexing process (see /src/snovault/indexing_views.py and /src/snovault/embed.py), the list of embedded fields for each type file are used to parse the fully embedded object down ONLY the embeds listed in the types file. This way, any linked objects that aren't explicitly referenced in this list are excluded.
The result of this is that the total amount of embedded data is much less after indexing.
Small changes in /src/snovault/auditor.py were also made to allow for embedding of objects that are defined within other objects and are thus not individual DB entries (i.e. no @id). Additionally, allows for embedding of non-object fields.

In addition, the ES mapping was made to conform to the list of embedded fields, too (/src/snovault/elasticsearch/create_mapping.py).
Other mapping changes included removal of boost, which wasn't working anyways.

* the 'links' field names contained dots ('.'). These were simply changed to '~'.

For example, the indexed 'links' field for an item would change from user.lab to user~lab.

The overall result of these changes is that the 'embedded' and 'links' properties of any item are changed post-indexing. ES mapping is changed pre-indexing.

TO TEST THIS BEHAVIOR, a test named 'test_selective_embedding' was added to src/snowflakes/tests/test_search.py 
