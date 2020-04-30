## High-level overview of search package

The search module sits between a user's search request and our ElasticSearch cluster. It is responsible for converting the query string from a user's request into an ElasticSearch query, executing the query, gathering the raw results, and formatting them in a way that the frontend expects. The general goal is to keep each of these steps as decoupled as possible so that it's easy to reuse and extend the functionality in new views. For examples it's possible to create a new way of building up a query or formatting raw results and drop them into place without modifying the surrounding components.

How this package is used to create Pyramid views can be seen in **src/snowflakes/search_views.py** and **src/snowflakes/tests/test_searchv2.py**. There are extensive tests for every component (e.g. **snovault/tests/test_searches_responses.py**) that document expected behavior.

### Object relationships

Pyramid view (**snowflakes.search_views.py**) -> renders *FieldedResponse* (**responses.py**) -> contains many *ResponseFields* (**fields.py**)

### Manifest of modules
* configs.py - Specialized helper classes for filtering parameters passed to certain ElasticSearch aggregations
* decorators.py - General helper decorators for exception handling and dict filtering
* defaults.py - Default parameters and templates used in query building (ALL_CAPS variables)
* __fields.py__ - Classes that define the fields that are returned in JSON, specifically subclasses for different types of searches, matrices, etc.
* interfaces.py - Constants that alias raw strings (ALL_CAPS variables)
* __mixins.py__ - Classes mixed in to *FieldedResponse* that allow for flexible formatting of raw ElasticSearch results
* __parsers.py__ - Class for parsing and maniuplating the query string passed in by the user
* __queries.py__ - Classes and subclasses for generating specific Elasticseach queries
* __responses.py__ - Classes for wrapping raw ElasticSearch responses (*QueryResponse* and subclasses) and Pyramid view responses (*FieldedReponse* and subclasses)

\* Note that bolded modules contain core functionality, unbolded are less important/contain implementation details.

### Longer Examples

Every *ResponseField* has a render() method that returns a dictionary with keys and values. The *ResponseFields* can be as simple or as complicated as you want as long as their render method returns a dictionary of data. The *FieldedResponse* merges all of the dictionaries from the *ResponseFields* it contains and passes it to Pyramid to render as a view.

A simple example just returns a hardcoded dictionary:

- *SimpleResponseField*.render() -> return {"@graph": [1, 2, 3]}

A real example:

- The *BasicSearchResponseField* splits up its rendering into three parts:
    - (1) Build an ElasticSearch query
    - (2) Execute the query and gather raw search results
    - (3) Format raw search results into dictionary used by frontend

All of these steps call other classes to do the work for them. 

**In the case of (1) Building an ElasticSearch query:**
-  The *BasicSearchResponseField* uses the *BasicQueryFactory* (**queries.py**) which inherits from *AbstractQueryFactory*
- *AbstractQueryFactory* has all of the basic methods required to build up an ElasticSearch query (e.g. adding filters, aggregations, etc.). It makes use of another class called *ParamsParser* (**parsers.py**) that makes parsing the query string passed in by the user convenient (and tested!). For example:
    - method to pull all of the params that match a key (e.g. field=status)
    - method to split params by whether they are equal (status=released) or not equal (status!=in progress)
    - method to split params by whether they are exists (s3_uri=\*) or not exists (s3_uri!=\*)
- *AbstractQueryFactory* has a method called build_query() that is not implemented. The point is to have the subclasses of *AbstractQueryFactory* use this method to define how they should build up their specific queries. The subclasses should make use of the common methods in the *AbstractQueryFactory* class or implement their own private helper methods. Ideally any additional functionality will be an extension (new class that wraps base functionality) rather than a revision of the base class.
- Example of steps in *BasicQueryFactory*.build_query():
    - validate_item_types() - checks to see if the types (e.g. Experiment, File, etc.) passed in by the user are valid types
    - add_simple_query_string_query() - adds searchTerm=... value to raw ES query
    - add_query_string_query() - adds advancedQuery=... value to raw ES query
    - add_filters() - adds filters such as status=released or assay_title=Chip-seq to raw ES query
    - add_post_filters() - adds permission filters based on viewing permission of user to raw ES query
    - add_source() - adds raw fields to return from ES (we store multiple views e.g. object, embedded in ES and only return a subset of fields)
    - add_slice() - adds limit=100 or limit=all to raw ES query
    - --> returns the raw ES query

**In the case of (2) Execute the query and gather raw search results:**
- passes the query built up in previous step to ES cluster
- raw results wrapped in *BasicQueryResponseWithFacets* (**responses.py**) which has mixins (**mixins.py**) for rendering specific raw output types (e.g. aggregations are converted from raw form to something that frontend expects, namely *facets*). Mixins allow for flexible extension of rendering capabilities based on context.

**In the case of (3) Format raw search results into dictionary used by frontend:**
- *BasicQueryResponseWithFacet* mixin methods (e.g. *to_hits*, *to_facets*) are called and return a dictionary with *@graph* and *facets* fields to the *BasicSearchResponseField*:
```
{"@graph": [{"accession": "ENCFFABC123"}, {"accession": "ENCFFABC456"}], "facets": [{"assay_titles": [1, 2, 3]}]}
```

The *BasicSearchResponseField* is only responsible for building and executing the query and formatting the raw response (all of which are delegated to other classes). There are other fields that we want to add to the JSON response, such as *title*, *@id*, *@type* etc. These are all individually encapsulated in their own *ResponseField* subclasses.

For example: *TitleResponseField*.render() --> return {"title": "Experiment"}

Some of these are very simple methods, but others are very complex/have multiple steps/dependencies. Having a common interface to all makes wrapping and rendering arbitrary code convenient. It also allows components to be reusable and insulated from changes in other parts of the system.

In the case of other types of ElasticSearch queries there are other subclasses of *AbstractQueryFactory* that build specific queries, e.g. *BasicMatrixQueryFactory* (**queries.py**). In many cases these share a lot of the same functionality as the parent classes but will have additional steps (e.g. adding specific aggregations needed by the Experiment matrix), or will override certain parent methods (e.g. raising an error if more than one type is specified in matrix query string).
